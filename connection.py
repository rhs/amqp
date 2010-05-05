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
from protocol import *

from framing import AMQP_FRAME, Frame, FrameDecoder, FrameEncoder
from codec import TypeDecoder, TypeEncoder
from util import Buffer, parse
from uuid import uuid4


PROTO_HDR_FMT = "!4sBBBB"
PROTO_HDR_SIZE = struct.calcsize(PROTO_HDR_FMT)
assert PROTO_HDR_SIZE == 8

class ConnectionError(Exception):
  pass

class Connection:

  def __init__(self, factory):
    self.id = "%X" % id(self)
    self.factory = factory
    self.tracing = set(os.environ.get("AMQP_TRACE", "err").split())
    self.input = Buffer()
    self.output = Buffer(struct.pack(PROTO_HDR_FMT, "AMQP", 0, 1, 0, 0))

    self.state = self.__proto_header

    self.frame_decoder = FrameDecoder()
    self.frame_encoder = FrameEncoder()

    self.type_decoder = TypeDecoder()
    self.type_encoder = TypeEncoder()

    for cls in CLASSES:
      self.type_encoder.deconstructors[cls] = lambda v: (v.DESCRIPTORS[0], v.deconstruct())
      for d in cls.DESCRIPTORS:
        self.type_decoder.constructors[d] = lambda d, v, c=cls: c(*v)

    self.open_rcvd = False
    self.open_sent = False
    self.close_rcvd = False
    self.close_sent = False

    # incoming channel -> session
    self.incoming = {}
    # outgoing channel -> session
    self.outgoing = {}

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

  def opened(self):
    return self.open_rcvd and self.open_sent and \
        not (self.close_rcvd or self.close_sent)

  def closed(self):
    self.close_rcvd and self.close_sent

  def closing(self):
    return self.close_rcvd and not self.close_sent

  def write(self, bytes):
    self.trace("raw", "RECV: %r", bytes)
    self.input.write(bytes)
    self.state = parse(self.state)

  def __proto_header(self):
    if self.input.pending() >= PROTO_HDR_SIZE:
      hdr = self.input.read(PROTO_HDR_SIZE)
      magic, proto, major, minor, revision = struct.unpack(PROTO_HDR_FMT, hdr)
      if (magic, proto, major, minor, revision) == ("AMQP", 0, 1, 0, 0):
        return self.__framing
      else:
        raise ValueError("bad protocol header")

  def __framing(self):
    self.frame_decoder.write(self.input.read())
    for f in self.frame_decoder.read():
      self.process_frame(f)

  def process_frame(self, f):
    body, remainder = self.type_decoder.decode(f.payload)
    assert remainder == ""
    self.trace("frm", "RECV[%s]: %s", f.channel, body)
    getattr(self, "do_%s" % body.NAME, self.unhandled)(f.channel, body)

  def unhandled(self, channel, op):
    ssn = self.incoming[channel]
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
    for ch, ssn in self.outgoing.items():
      ssn.tick()
      for body in ssn.read():
        self.post_frame(ssn.channel, body)

  def post_frame(self, channel, body):
    assert not self.close_sent
    self.trace("frm", "SENT[%s]: %s", channel, body)
    f = Frame(AMQP_FRAME, channel, None, self.type_encoder.encode(body))
    self.frame_encoder.write(f)
    self.output.write(self.frame_encoder.read())

  def open(self, *args, **kwargs):
    self.post_frame(0, Open(*args, **kwargs))
    self.open_sent = True

  def do_open(self, channel, open):
    if self.open_rcvd:
      self.close(ConnectionError(error_code=501, description="double open"))
    else:
      self.open_rcvd = True

  def close(self, *args, **kwargs):
    # avoid stranding frames inside sessions
    self.tick()
    self.post_frame(0, Close(*args, **kwargs))
    self.close_sent = True

  def do_close(self, channel, close):
    if not self.close_rcvd:
      self.close_rcvd = True

  def add(self, ssn):
    ssn.channel = self.allocate_channel()
    self.outgoing[ssn.channel] = ssn

  def allocate_channel(self):
    return max([-1] + self.outgoing.keys()) + 1

  def remove(self, ssn):
    # avoid stranding frames inside sessions
    self.tick()
    if ssn.channel in self.outgoing and self.outgoing[ssn.channel] == ssn:
      del self.outgoing[ssn.channel]
      ssn.channel = None
    else:
      raise ConnectionError("no such session")

  def do_begin(self, channel, begin):
    if channel in self.incoming:
      raise ConnectionError("double begin")

    if begin.remote_channel in self.outgoing:
      ssn = self.outgoing[begin.remote_channel]
    else:
      ssn = self.factory(begin.name)
      ssn.remote_channel = channel
      self.add(ssn)

    self.incoming[channel] = ssn
    ssn.write(begin)

  def do_end(self, channel, end):
    if channel not in self.incoming:
      raise ConnectionError("double end")

    ssn = self.incoming.pop(channel)
    ssn.write(end)
