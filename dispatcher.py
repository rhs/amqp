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

import os, struct, sys

from framing import AMQP_FRAME, Frame, encode, decode
from util import Buffer, parse


PROTO_HDR_FMT = "!4sBBBB"
PROTO_HDR_SIZE = struct.calcsize(PROTO_HDR_FMT)
assert PROTO_HDR_SIZE == 8

class Dispatcher:

  def __init__(self, protocol_id, frame_type):
    self.protocol_id = protocol_id
    self.frame_type = frame_type
    self.id = "%X" % id(self)
    self._tracing = set()
    self.tracing(*os.environ.get("AMQP_TRACE", "").split())
    self.multiline = False
    self.input = Buffer()
    self.output = Buffer(struct.pack(PROTO_HDR_FMT, "AMQP", self.protocol_id, 1, 0, 0))

    self.state = self.__proto_header

  def tracing(self, *args, **kwargs):
    names = set(args)
    for n in kwargs:
      if kwargs[n]: names.add(n)
    if "err" not in kwargs:
      names.add("err")
    self._tracing = names

  def trace(self, category, format, *args):
    if category in self._tracing:
      prefix = "[%s %s]" % (self.id, category)
      if args:
        message = format % args
      else:
        message = format
      print >> sys.stderr, prefix, \
          message.replace(os.linesep, "%s%s " % (os.linesep, prefix))

  def write(self, bytes):
    self.trace("raw", "RECV: %r", bytes)
    self.input.write(bytes)
    self.state = parse(self.state)

  def __proto_header(self):
    if self.input.pending() >= PROTO_HDR_SIZE:
      hdr = self.input.read(PROTO_HDR_SIZE)
      magic, proto, major, minor, revision = struct.unpack(PROTO_HDR_FMT, hdr)
      if (magic, proto, major, minor, revision) == ("AMQP", self.protocol_id, 1, 0, 0):
        return self.__framing
      else:
        raise ValueError("bad protocol header")

  def __framing(self):
    while True:
      f, n = decode(self.input.peek())
      if f:
        self.input.read(n)
        state = self.process_frame(f)
        if state is not None:
          return state
      else:
        break

  def process_frame(self, f):
    body, remainder = self.type_decoder.decode(f.payload)
    assert remainder == ""
    self.trace("frm", "RECV[%s]: %s", f.channel, body.format(self.multiline))
    return getattr(self, "do_%s" % body.NAME, self.unhandled)(f.channel, body)

  def post_frame(self, channel, body):
    self.trace("frm", "SENT[%s]: %s", channel, body.format(self.multiline))
    f = Frame(self.frame_type, channel, None, self.type_encoder.encode(body))
    self.output.write(encode(f))

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
