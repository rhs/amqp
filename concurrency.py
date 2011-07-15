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

import inspect, sys, time
from select import select

def synchronized(meth):
  args, vargs, kwargs, defs = inspect.getargspec(meth)
  scope = {}
  scope["meth"] = meth
  exec """
def %s%s:
  %s
  %s._lock.acquire()
  try:
    return meth%s
  finally:
    %s._lock.release()
""" % (meth.__name__, inspect.formatargspec(args, vargs, kwargs, defs),
       repr(inspect.getdoc(meth)), args[0],
       inspect.formatargspec(args, vargs, kwargs, defs,
                             formatvalue=lambda x: ""),
       args[0]) in scope
  return scope[meth.__name__]

class common_selectable_waiter:

  def wakeup(self):
    self._do_write()

  def wait(self, timeout=None):
    if timeout is not None:
      ready, _, _ = select([self], [], [], timeout)
    else:
      ready = True

    if ready:
      self._do_read()
      return True
    else:
      return False

  def reading(self):
    return True

  def readable(self, selector):
    self._do_read()

if sys.platform in ('win32', 'cygwin'):
  import socket

  class selectable_waiter(common_selectable_waiter):

    def __init__(self):
      listener = socket.socket()
      listener.bind(('', 0))
      listener.listen(1)
      _, port = listener.getsockname()
      write_sock = socket.socket()
      write_sock.connect(("127.0.0.1", port))
      read_sock, _ = listener.accept()
      listener.close()

      self.read_sock = read_sock
      self.write_sock = write_sock

    def _do_write(self):
      self.write_sock.send("\0")

    def _do_read(self):
      self.read_sock.recv(65536)

    def fileno(self):
      return self.read_sock.fileno()

    def close(self):
      if self.write_sock is not None:
        self.write_sock.close()
        self.write_sock = None
        self.read_sock.close()
        self.read_sock = None

    def __del__(self):
      self.close()

  def __repr__(self):
    return "selectable_waiter<%r, %r>" % (self.read_sock, self.write_sock)
else:
  import os

  class selectable_waiter(common_selectable_waiter):

    def __init__(self):
      self.read_fd, self.write_fd = os.pipe()

    def _do_write(self):
      os.write(self.write_fd, "\0")

    def _do_read(self):
      os.read(self.read_fd, 65536)

    def fileno(self):
      return self.read_fd

    def close(self):
      if self.write_fd is not None:
        os.close(self.write_fd)
        self.write_fd = None
        os.close(self.read_fd)
        self.read_fd = None

    def __del__(self):
      self.close()

    def __repr__(self):
      return "selectable_waiter<%r, %r>" % (self.read_fd, self.write_fd)

del common_selectable_waiter

class Waiter(object):

  def __init__(self, condition):
    self.condition = condition

  def wait(self, predicate, timeout=None):
    passed = 0
    start = time.time()
    while not predicate():
      if timeout is None:
        # XXX: this timed wait thing is not necessary for the fast
        # condition from this module, only for the condition impl from
        # the threading module

        # using the timed wait prevents keyboard interrupts from being
        # blocked while waiting
        self.condition.wait(3)
      elif passed < timeout:
        self.condition.wait(timeout - passed)
      else:
        return bool(predicate())
      passed = time.time() - start
    return True

  def notify(self):
    self.condition.notify()

  def notifyAll(self):
    self.condition.notifyAll()

class Condition:

  def __init__(self, lock):
    self.lock = lock
    self.waiters = []
    self.waiting = []

  def notify(self):
    assert self.lock._is_owned()
    if self.waiting:
      self.waiting[0].wakeup()

  def notifyAll(self):
    assert self.lock._is_owned()
    for w in self.waiting:
      w.wakeup()

  def wait(self, timeout=None):
    assert self.lock._is_owned()
    if not self.waiters:
      self.waiters.append(selectable_waiter())
    sw = self.waiters.pop(0)
    self.waiting.append(sw)
    try:
      st = self.lock._release_save()
      sw.wait(timeout)
    finally:
      self.lock._acquire_restore(st)
      self.waiting.remove(sw)
      self.waiters.append(sw)
