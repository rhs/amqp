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

AMQP_FRAME = 0x00
SASL_FRAME = 0x01
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

def encode(frame):
  extended = frame.extended or ""
  padd = len(extended) % 4
  if padd: extended += "\x00"*(4-padd)
  size = FRAME_HDR_SIZE + len(extended) + len(frame.payload)
  doff = (FRAME_HDR_SIZE + len(extended))/4
  header = struct.pack(FRAME_HDR_FMT, size, doff, frame.type, frame.channel)
  return "%s%s%s" % (header, extended, frame.payload)

def decode(bytes):
  if len(bytes) >= FRAME_HDR_SIZE:
    hdr = bytes[:FRAME_HDR_SIZE]
    size, doff, type, channel = struct.unpack(FRAME_HDR_FMT, hdr)
    if len(bytes) >= size:
      extended = bytes[FRAME_HDR_SIZE:doff*4]
      payload = bytes[doff*4:size]
      return Frame(type, channel, extended, payload), size
  return None, 0
