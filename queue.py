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

TAIL = Constant("TAIL")

class Entry:

  def __init__(self, queue, item):
    self.queue = queue
    self.item = item
    self.next = None
    self.acquired = False

  def tail(self):
    return self.item is TAIL

  def append(self, item):
    assert self.next is None
    self.next = Entry(self.queue, item)
    self.queue.size += 1
    return self.next

  def remove(self):
    self.item = None
    self.queue.size -= 1
    self.queue.compact()

  def acquire(self):
    if self.acquired:
      return False
    else:
      self.acquired = True
      return True

  def __repr__(self):
    return "Entry(acquired=%r, item=%r)" % (self.acquired, self.item)

class Queue:

  def __init__(self, threshold=None, ring=None, acquire=True, dequeue=True):
    self.tail = Entry(self, TAIL)
    self.head = self.tail
    self.size = 0
    self.threshold = threshold
    self.ring = ring
    self.acquire = acquire
    self.dequeue = dequeue

  def capacity(self):
    return self.threshold == None or self.size < self.threshold

  def put(self, item):
    entry = self.tail
    self.tail = self.tail.append(TAIL)
    entry.item = item
    if self.ring is not None and self.size > self.ring:
      self.head = self.head.next

  def compact(self):
    while self.head.item is None:
      self.head = self.head.next

  def source(self):
    return Source(self.head, self.acquire, self.dequeue)

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

  def __init__(self, next, acquire, dequeue):
    self.next = next
    self.acquire = acquire
    self.dequeue = dequeue
    self.unacked = {}
    self.tag = 0

  def get(self):
    if self.acquire:
      while self.next.item is None or self.next.acquired:
        self.next = self.next.next

    entry = self.next
    if entry.tail():
      return None, None
    self.next = self.next.next
    if self.acquire:
      entry.acquire()

    self.tag += 1
    tag = str(self.tag)
    self.unacked[tag] = entry

    return tag, entry.item

  def settle(self, tag, outcome):
    entry = self.unacked.pop(tag)
    if self.dequeue:
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
