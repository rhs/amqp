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

from util import Constant

HEAD = Constant("HEAD")
TAIL = Constant("TAIL")

class Entry:

  def __init__(self, item):
    self.item = item
    self.next = None
    self.prev = None
    self.acquired = False

  def tail(self):
    return self.item is TAIL

  def head(self):
    return self.item is HEAD

  def insert(self, item):
    entry = Entry(item)
    next = self.next
    entry.next = next
    if next:
      next.prev = entry
    entry.prev = self
    self.next = entry
    return entry

  def remove(self):
    next = self.next
    prev = self.prev

    if next:
      next.prev = prev
    if prev:
      prev.next = next

  def acquire(self):
    if self.acquired:
      return False
    else:
      self.acquired = True
      return True

  def __repr__(self):
    return "Entry(acquired=%r, item=%r)" % (self.acquired, self.item)

class Queue:

  def __init__(self, threshold=None):
    self.head = Entry(HEAD)
    self.tail = self.head.insert(TAIL)
    self.size = 0
    self.threshold = threshold

  def capacity(self):
    return self.threshold == None or self.size < self.threshold

  def put(self, item):
    self.tail.prev.insert(item)
    self.size += 1

  def source(self):
    return Source(self.head.next)

  def target(self):
    return Target(self)

  def __repr__(self):
    entries = []
    e = self.head
    while e is not None:
      entries.append(e)
      e = e.next
    return repr(entries)

class Source:

  def __init__(self, next):
    self.next = next
    self.unacked = {}
    self.tag = 0

  def get(self):
    while self.next.acquired:
      self.next = self.next.next

    entry = self.next
    if entry.tail():
      return None, None
    self.next = self.next.next
    entry.acquire()

    self.tag += 1
    tag = str(self.tag)
    self.unacked[tag] = entry

    return tag, entry.item

  def settle(self, tag, outcome):
    entry = self.unacked.pop(tag)
    entry.remove()
    print "DEQUEUED:", tag, outcome
    return "DEQUEUED"

class Target:

  def __init__(self, queue):
    self.queue = queue

  def capacity(self):
    return self.queue.capacity()

  def put(self, tag, message):
    self.queue.put(message)
    print "ENQUEUED:", tag, message.fragments

  def settle(self, tag):
    return "ENQUEUED"
