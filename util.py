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

import os, mllib, traceback

__SELF__ = object()

class Constant:

  def __init__(self, name, value=__SELF__):
    self.name = name
    if value is __SELF__:
      self.value = self
    else:
      self.value = value

  def __repr__(self):
    return self.name

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

  def peek(self, n=None):
    if n is None:
      return self.bytes
    else:
      return self.bytes[:n]

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
  if name is None:
    return name
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

class ConnectionSelectable:

  def __init__(self, socket, connection, tick):
    self.socket = socket
    self.connection = connection
    self.tick = tick

  def fileno(self):
    return self.socket.fileno()

  def timing(self):
    return None

  def reading(self):
    return self.socket is not None

  def writing(self):
    if self.socket is None: return False
    self.tick(self.connection)
    return self.connection.pending()

  def readable(self):
    # XXX: hardcoded buffer size
    try:
      bytes = self.socket.recv(64*1024)
      if bytes:
        self.connection.write(bytes)
        self.tick(self.connection)
        return
    except:
      self.connection.trace("err", traceback.format_exc().strip())
      # XXX: need to signal connection so it can cleanup links
    self.socket.close()
    # XXX: need to unregister
    self.socket = None

  def writeable(self):
    try:
      n = self.socket.send(self.connection.peek())
      bytes = self.connection.read(n)
      return
    except:
      self.connection.trace("err", traceback.format_exc().strip())
      # XXX: need to signal connection so it can cleanup links
    self.socket.close()
    # XXX: need to unregister
    self.socket = None

class Range:

  def __init__(self, lower, upper = None):
    self.lower = lower
    if upper is None:
      self.upper = self.lower
    else:
      self.upper = upper
    assert self.lower <= self.upper

  def __contains__(self, n):
    return self.lower <= n and n <= self.upper

  def __iter__(self):
    i = self.lower
    while i <= self.upper:
      yield i
      i += 1

  def touches(self, r):
    # XXX: are we doing more checks than we need?
    return (self.lower - 1 in r or
            self.upper + 1 in r or
            r.lower - 1 in self or
            r.upper + 1 in self or
            self.lower in r or
            self.upper in r or
            r.lower in self or
            r.upper in self)

  def span(self, r):
    return Range(min(self.lower, r.lower), max(self.upper, r.upper))

  def intersect(self, r):
    lower = max(self.lower, r.lower)
    upper = min(self.upper, r.upper)
    if lower > upper:
      return None
    else:
      return Range(lower, upper)

  def __repr__(self):
    return "%s-%s" % (self.lower, self.upper)

class RangeSet:

  def __init__(self, *args):
    self.ranges = []
    for n in args:
      self.add(n)

  def __contains__(self, n):
    for r in self.ranges:
      if n in r:
        return True
    return False

  def add_range(self, range):
    idx = 0
    while idx < len(self.ranges):
      r = self.ranges[idx]
      if range.touches(r):
        del self.ranges[idx]
        range = range.span(r)
      elif range.upper < r.lower:
        self.ranges.insert(idx, range)
        return
      else:
        idx += 1
    self.ranges.append(range)

  def add(self, lower, upper = None):
    self.add_range(Range(lower, upper))

  def empty(self):
    for r in self.ranges:
      if r.lower <= r.upper:
        return False
    return True

  def max(self):
    if self.ranges:
      return self.ranges[-1].upper
    else:
      return None

  def min(self):
    if self.ranges:
      return self.ranges[0].lower
    else:
      return None

  def __iter__(self):
    return iter(self.ranges)

  def __repr__(self):
    return str(self.ranges)
