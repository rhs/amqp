#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import inspect, mllib, os, struct
from operations import *

from framing import CONN_FRAME, FrameDecoder, FrameEncoder
from codec import TypeDecoder, TypeEncoder
from util import Buffer, parse
from uuid import uuid4


PROTO_HDR_SIZE=8

class Connection:

  def __init__(self, factory):
    self.id = "%X" % id(self)
    self.factory = factory
    self.tracing = set(os.environ.get("AMQP_TRACE", "err").split())
    self.input = Buffer()
    self.output = Buffer("AMQP\x00\x01\x00\x00")

    self.state = self.__proto_header

    self.frame_decoder = FrameDecoder()
    self.frame_encoder = FrameEncoder()

    self.type_decoder = TypeDecoder()
    self.type_encoder = TypeEncoder()

    for cls in COMPOUND:
      self.type_encoder.deconstructors[cls] = lambda v: (v.DESCRIPTORS[0], v.deconstruct())
      for d in cls.DESCRIPTORS:
        self.type_decoder.constructors[d] = lambda d, v, c=cls: c(*v)

    self.open_rcvd = False
    self.open_sent = False
    self.close_rcvd = False
    self.close_sent = False

    self.sessions = {}
    self.channels = {}

  def trace(self, category, format, *args):
    if category in self.tracing:
      prefix = "[%s %s]" % (self.id, category)
      if args:
        message = format % args
      else:
        message = format
      print prefix, message.replace(os.linesep, "%s%s " % (os.linesep, prefix))

  def opening(self):
    return self.open_rcvd and not self.open_sent

  def closing(self):
    return self.close_rcvd and not self.close_sent

  def write(self, bytes):
    self.trace("raw", "RECV: %r", bytes)
    self.input.write(bytes)
    self.state = parse(self.state)

  def __proto_header(self):
    if self.input.pending() >= PROTO_HDR_SIZE:
      hdr = self.input.read(PROTO_HDR_SIZE)
      magic, proto, major, minor, revision = struct.unpack("!4sBBBB", hdr)
      if (magic, proto, major, minor, revision) == ("AMQP", 0, 1, 0, 0):
        return self.__framing
      else:
        raise ValueError("bad protocol header")

  def __framing(self):
    self.frame_decoder.write(self.input.read())
    for f in self.frame_decoder.read():
      self.process_frame(f)

  def process_frame(self, f):
    if f.payload:
      op, _ = self.type_decoder.decode(f.payload)
    else:
      op = Empty()
    op.init(f)
    self.trace("ops", "RECV: %s", op)
    getattr(self, "do_%s" % op.NAME, self.unhandled)(op)

  def unhandled(self, op):
    ssn = self.channels[op.channel]
    ssn.write(op)

  def read(self, n=None):
    self.tick()
    result = self.output.read(n)
    self.trace("raw", "SENT: %r", result)
    return result

  def peek(self, n=None):
    return self.output.peek(n)

  def pending(self):
    self.tick()
    return self.output.pending()

  def tick(self):
    for ssn in self.sessions.values():
      ssn.tick()
      for op in ssn.read():
        op.channel = ssn.channel
        self.write_op(op)

  def write_op(self, op):
    assert not self.close_sent
    self.trace("ops", "SENT: %s", op)
    f = op.frame(self.type_encoder)
    self.frame_encoder.write(f)
    self.output.write(self.frame_encoder.read())

  def open(self, *args, **kwargs):
    self.write_op(Open(*args, **kwargs))
    self.open_sent = True

  def do_open(self, open):
    if self.open_rcvd:
      self.close(ConnectionError(error_code=501, description="double open"))
    else:
      self.open_rcvd = True

  def close(self, *args, **kwargs):
    # avoid stranding frames inside sessions
    self.tick()
    self.write_op(Close(*args, **kwargs))
    self.close_sent = True

  def do_close(self, close):
    if not self.close_rcvd:
      self.close_rcvd = True

  def do_attach(self, attach):
    if self.sessions.has_key(attach.name):
      ssn = self.sessions[attach.name]
    else:
      ssn = self.factory(attach.name)
      self.add(ssn)
    self.channels[attach.channel] = ssn
    ssn.write(attach)

  def do_detach(self, detach):
    ssn = self.channels.pop(detach.channel)
    ssn.write(detach)

  def add(self, ssn):
    ssn.channel = self.allocate_channel()
    self.sessions[ssn.name] = ssn

  def allocate_channel(self):
    return max([-1] + [s.channel for s in self.sessions.values()]) + 1

  def remove(self, ssn):
    # avoid stranding frames inside the session
    self.tick()
    del self.sessions[ssn.name]
