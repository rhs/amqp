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

import os, mllib

class InsufficientCapacity(Exception): pass

class Buffer:

  def __init__(self, bytes="", capacity=None):
    self.bytes = bytes
    self.capacity = capacity

  def read(self, n=None):
    if n is None:
      n = len(self.bytes)
    result = self.bytes[:n]
    self.bytes = self.bytes[n:]
    return result

  def write(self, bytes):
    if self.capacity and len(self.bytes) + len(bytes) > self.capacity:
      raise InsufficientCapacity()
    self.bytes += bytes

  def pending(self):
    return len(self.bytes)

def parse(state):
  while True:
    next = state()
    if next is None:
      break
    else:
      state = next
  return state

def load_xml(name):
  xml_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
  return mllib.xml_parse(xml_file)

def pythonize(name, camel=False):
  name = str(name)
  if camel:
    return "".join([p.capitalize() for p in name.split("-")])
  else:
    return name.replace("-", "_")

def decode_numeric_desc(descriptor):
  high, low = [int(x, 0) for x in descriptor.split(":")]
  return (high << 32) + low

def identity(x):
  return x
