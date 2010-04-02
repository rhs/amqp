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
from threading import RLock, Condition

NON_DESTRUCTIVE = "NON_DESTRUCTIVE"
DESTRUCTIVE = "DESTRUCTIVE"

class Entry:

  def __init__(self, queue, item):
    self.queue = queue
    self.item = item
    self.next = None
    self.acquired = False

  def add(self, item):
    assert self.next is None
    self.next = Entry(self.queue, item)

  def acquire(self):
    if self.acquired:
      return False
    else:
      self.acquired = True
      return True

  def dequeue(self):
    self.item = None
    self.queue.gc()

  def is_garbage(self):
    return self.item == None

  def __repr__(self):
    return "Entry(acquired=%r, item=%r)" % (self.acquired, self.item)

class Queue:

  def __init__(self):
    self.tail = Entry(self, None)
    self.head = self.tail
    self.head.acquire()

  def put(self, item):
    self.tail.add(item)
    self.tail = self.tail.next

  def gc(self):
    entry = self.head
    while entry.next is not None and entry.next.next is not None:
      if entry.next.is_garbage():
        entry.next = entry.next.next
      entry = entry.next

  def cursor(self, mode, filter=lambda x: True):
    return Cursor(self, mode, filter)

  def __repr__(self):
    entries = []
    e = self.head
    while e is not None:
      entries.append(e)
      e = e.next
    return repr(entries)

class Cursor:

  def __init__(self, queue, mode, filter):
    self.queue = queue
    self.mode = mode
    self.filter = filter
    self.head = self.queue.head

  def get(self, peek=False):
    e = self.head
    result = None
    while e.next is not None:
      e = e.next
      if not e.is_garbage() and not e.acquired and self.filter(e.item):
        # XXX
        if (self.mode == NON_DESTRUCTIVE or
            (peek and not e.acquired) or
            (not peek and e.acquire())):
          result = e
          break
    if not peek:
      self.head = e
    return result

  def peek(self):
    return self.get(peek=True)

  def close(self):
    # XXX: TODO
    pass

  def __repr__(self):
    return "Cursor(mode=%r, filter=%r, head=%r)" % \
        (self.mode, self.filter, self.head)

if __name__ == '__main__':
  q = Queue()

  q.put(1)
  q.put(2)
  q.put(3)

  c1 = q.cursor(DESTRUCTIVE, lambda x: x == 1)
  c2 = q.cursor(DESTRUCTIVE, lambda x: x == 2)
  c3 = q.cursor(DESTRUCTIVE, lambda x: x == 3)

  e2 = c2.get()
  print e2, c2, q
  e2.dequeue()
  print e2, c2, q

  e3 = c3.get()
  print e3, c3, q
  e3.dequeue()
  print e3, c3, q

  e1 = c1.get()
  print e1, c1, q
  e1.dequeue()
  print e1, c1, q
