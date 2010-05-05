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

AMQP_FRAME = 0
FRAME_HDR_FMT = "!I2BH"
FRAME_HDR_SIZE = struct.calcsize(FRAME_HDR_FMT)
assert FRAME_HDR_SIZE == 8

class Frame:

  def __init__(self, type, channel, extended, payload):
    self.type = type
    self.channel = channel
    self.extended = extended
    self.payload = payload

  def __repr__(self):
    return "Frame(%s, %s, %r, %r)" % \
        (self.type, self.channel, self.extended, self.payload)

class FrameEncoder:

  def __init__(self):
    self.buffer = Buffer()

  def write(self, frame):
    self.buffer.write(self.encode(frame))

  def encode(self, frame):
    extended = frame.extended or ""
    padd = len(extended) % 4
    if padd: extended += "\x00"*(4-padd)
    size = FRAME_HDR_SIZE + len(extended) + len(frame.payload)
    doff = (FRAME_HDR_SIZE + len(extended))/4
    header = struct.pack(FRAME_HDR_FMT, size, doff, frame.type, frame.channel)
    return "%s%s%s" % (header, extended, frame.payload)

  def read(self, n=None):
    return self.buffer.read(n)

class FrameDecoder:

  def __init__(self):
    self.input = Buffer()
    self.output = []

    self.state = self.__frame_header

    self.size = None
    self.doff = None
    self.type = None
    self.channel = None
    self.extended = None

  def write(self, bytes):
    self.input.write(bytes)
    self.state = parse(self.state)

  def __frame_header(self):
    if self.input.pending() >= FRAME_HDR_SIZE:
      st = self.input.read(FRAME_HDR_SIZE)
      self.size, self.doff, self.type, self.channel = \
          struct.unpack(FRAME_HDR_FMT, st)
      return self.__frame_extended

  def __frame_extended(self):
    size = self.doff*4 - FRAME_HDR_SIZE
    if self.input.pending() >= size:
      self.extended = self.input.read(size)
      return self.__frame_body

  def __frame_body(self):
    size = self.size - self.doff*4
    if self.input.pending() >= size:
      payload = self.input.read(size)
      frame = Frame(self.type, self.channel, self.extended, payload)
      self.output.append(frame)
      return self.__frame_header

  def read(self, n=None):
    result = self.output[:n]
    del self.output[:n]
    return result
