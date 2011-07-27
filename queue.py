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

from protocol import ACCEPTED
from util import Constant

TAIL = Constant("TAIL")

class Entry:

  def __init__(self, queue, id, item):
    self.queue = queue
    self.id = id
    self.item = item
    self.next = None
    self.acquired = None

  def tail(self):
    return self.item is TAIL

  def append(self, item):
    assert self.next is None
    self.next = Entry(self.queue, self.queue.identify(), item)
    self.queue.size += 1
    return self.next

  def remove(self):
    self.item = None
    self.queue.size -= 1
    self.queue.compact()

  def acquire(self, owner):
    if not self.acquired:
      self.acquired = owner
    return self.acquired

  def release(self):
    self.acquired = None

  def __del__(self):
    if self.item is not None:
      self.queue.size -= 1

  def __repr__(self):
    return "Entry(id=%s, item=%r, %s)" % (self.id, self.item, self.acquired)

class Queue:

  def __init__(self, threshold=None, ring=None, acquire=True, dequeue=True):
    self.next_id = 0
    self.tail = Entry(self, self.identify(), TAIL)
    self.head = self.tail
    self.size = 0
    self.threshold = threshold
    self.ring = ring
    self.acquire = acquire
    self.dequeue = dequeue

  def identify(self):
    id = self.next_id
    self.next_id += 1
    return id

  def capacity(self):
    return self.threshold == None or self.size < self.threshold

  def put(self, item):
    entry = self.tail
    self.tail = self.tail.append(TAIL)
    entry.item = item
    if self.ring is not None and self.size > self.ring:
      self.head = self.head.next
    return entry

  def compact(self):
    while self.head.item is None:
      self.head = self.head.next

  def source(self):
    return Source(self.head, self.acquire, self.dequeue)

  def target(self):
    return Target(self)

  def entries(self):
    e = self.head
    while e is not None:
      yield e
      e = e.next

  def __repr__(self):
    return repr(list(self.entries()))

class Terminus:

  def __init__(self):
    self._durable = False

  def configure(self, definition):
    self._durable = definition.durable
    return definition

  def durable(self):
    return self._durable

  def orphaned(self):
    return False

class Source(Terminus):

  def __init__(self, next, acquire, dequeue):
    Terminus.__init__(self)
    self.next = next
    self.acquire = acquire
    self.dequeue = dequeue
    self.unacked = {}

  def get(self):
    while (self.next.next and
           (self.next.item is None or
            (self.acquire and self.next.acquire(self) != self))):
      self.next = self.next.next

    entry = self.next
    if entry.tail():
      return None, None
    self.next = self.next.next

    if self.acquire:
      assert entry.acquired == self

    tag = str(entry.id)
    self.unacked[tag] = entry

    return tag, entry.item

  def resume(self, unsettled):
    for tag, state in unsettled.items():
      self.settle(tag, state)
    oldest = self.next
    for entry in self.unacked.values():
      if entry.id < oldest.id:
        oldest = entry
#    print "RESUME: %s -> %s" % (self.next.id, oldest.id)
    self.next = oldest
    # XXX: clear unacked?

  def settle(self, tag, state):
    entry = self.unacked.pop(tag)

    if state is None:
      entry.release()
    elif self.dequeue:
      entry.remove()
#      print "DEQUEUED:", tag, outcome

    return state

  def close(self):
    for tag in self.unacked.keys():
      # XXX: default outcome
      self.settle(tag, None)

class Target(Terminus):

  def __init__(self, queue):
    Terminus.__init__(self)
    self.queue = queue
    self.unsettled = {}

  def capacity(self):
    return self.queue.capacity()

  def put(self, tag, message, owner=None):
    if tag == "dump":
      print "-------- DUMP START --------"
      for e in self.queue.entries():
        print e
      print "-------- DUMP END   --------"
    else:
      entry = self.queue.put(message)
      entry.acquire(owner)
      self.unsettled[tag] = entry
      return ACCEPTED
#    print "ENQUEUED:", tag, message.fragments

  def resume(self, unsettled):
    pass

  def settle(self, tag, state):
    if tag == "dump":
      return state
    entry = self.unsettled.pop(tag)
    if state is None:
      entry.remove()
    else:
      entry.release()
    return state

  def close(self):
    # XXX: ???
    pass
