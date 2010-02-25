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

import struct
from util import Buffer, parse

CONN_FRAME = 0
SSN_FRAME = 1
FRAME_HDR = 24

class Frame:

  def __init__(self, type, flags, channel, payload):
    self.type = type
    self.flags = flags
    self.channel = channel
    self.payload = payload

class SessionFrame(Frame):

  def __init__(self, flags, channel, acknowledged, executed, capacity,
               command_id, payload):
    Frame.__init__(self, SSN_FRAME, flags, channel, payload)
    self.acknowledged = acknowledged
    self.executed = executed
    self.capacity = capacity
    self.command_id = command_id

  def __repr__(self):
    args = (self.flags, self.channel, self.executed, self.capacity,
            self.command_id, self.payload)
    return "SessionFrame(%s)" % ", ".join(map(repr, args))

class ConnectionFrame(Frame):

  def __init__(self, flags, channel, payload):
    Frame.__init__(self, CONN_FRAME, flags, channel, payload)

  def __repr__(self):
    args = (self.flags, self.channel, self.payload)
    return "ConnectionFrame(%s)" % ", ".join(map(repr, args))

class FrameEncoder:

  def __init__(self):
    self.buffer = Buffer()
    self.encoders = {
      SSN_FRAME: self.encode_ssn,
      CONN_FRAME: self.encode_conn
      }

  def write(self, frame):
    self.buffer.write(self.encode(frame))

  def encode(self, frame):
    return self.encoders[frame.type](frame)

  def encode_ssn(self, frame):
    header = struct.pack("!I2BH4I", FRAME_HDR + len(frame.payload), frame.type,
                         frame.flags, frame.channel, frame.acknowledged,
                         frame.executed, frame.capacity, frame.command_id)
    return "%s%s" % (header, frame.payload)

  def encode_conn(self, frame):
    header = struct.pack("!I2BH16x", FRAME_HDR + len(frame.payload),
                         frame.type, frame.flags, frame.channel)
    return "%s%s" % (header, frame.payload)

  def read(self, n=None):
    return self.buffer.read(n)

class FrameDecoder:

  def __init__(self):
    self.input = Buffer()
    self.output = []

    self.state = self.__frame_header

    self.size = None
    self.type = None
    self.flags = None
    self.channel = None
    self.control = None

    self.decoders = {
      SSN_FRAME: self.__decode_ssn,
      CONN_FRAME: self.__decode_conn
      }

  def write(self, bytes):
    self.input.write(bytes)
    self.state = parse(self.input, self.state)

  def __frame_header(self):
    if self.input.pending() >= FRAME_HDR:
      st = self.input.read(FRAME_HDR)
      self.size, self.type, self.flags, self.channel, self.control = \
          struct.unpack("!I2BH16s", st)
      return self.__frame_body

  def __frame_body(self):
    size = self.size - FRAME_HDR
    if self.input.pending() >= size:
      payload = self.input.read(size)
      frame = self.decoders[self.type](payload)
      self.output.append(frame)
      return self.__frame_header

  def __decode_ssn(self, payload):
    acknowledged, executed, capacity, command_id = \
        struct.unpack("!4I", self.control)
    return SessionFrame(self.flags, self.channel, acknowledged, executed,
                        capacity, command_id, payload)

  def __decode_conn(self, payload):
    return ConnectionFrame(self.flags, self.channel, payload)

  def read(self, n=None):
    result = self.output[:n]
    del self.output[:n]
    return result
