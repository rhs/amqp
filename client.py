#!/usr/bin/python
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

import socket
from connection import Connection as BaseConnection
from session import Session as BaseSession, SessionError
from link import Link as BaseLink, Sender as BaseSender, \
    Receiver as BaseReceiver, LinkError, link
from selector import Selector
from util import ConnectionSelectable, Constant
from concurrency import synchronized, Condition, Waiter
from threading import RLock
from uuid import uuid4
from protocol import Fragment, Linkage

class Timeout(Exception):
  pass

DEFAULT = Constant("DEFAULT")

class Connection(BaseConnection):

  def __init__(self):
    BaseConnection.__init__(self, self.session)
    self._lock = RLock()
    self.condition = Condition(self._lock)
    self.waiter = Waiter(self.condition)
    self.selector = Selector.default()
    self.timeout = 120

  def connect(self, host, port):
    sock = socket.socket()
    sock.connect((host, port))
    sock.setblocking(0)
    self.selector.register(ConnectionSelectable(sock, self, self._tick))

  @synchronized
  def _tick(self, connection):
    connection.tick()
    self.waiter.notify()

  def wait(self, predicate, timeout=DEFAULT):
    if timeout is DEFAULT:
      timeout = self.timeout
    self.selector.wakeup()
    if not self.waiter.wait(predicate, timeout):
      raise Timeout()

  def session(self, name = None):
    if name is None:
      name = str(uuid4())
    ssn = Session(self, name)
    self.add(ssn)
    ssn.begin()
    return ssn

  @synchronized
  def close(self):
    BaseConnection.close(self)
    self.wait(lambda: self.close_rcvd)

class Session(BaseSession):

  def __init__(self, connection, name):
    BaseSession.__init__(self, name, link)
    self.connection = connection
    self._lock = self.connection._lock
    self.timeout = 120

  def wait(self, predicate, timeout=DEFAULT):
    if timeout is DEFAULT:
      self.timeout = timeout
    self.connection.wait(predicate, timeout)

  @synchronized
  def sender(self, target):
    snd = Sender(self.connection, target)
    self.add(snd)
    snd.attach()
    self.wait(lambda: snd.opened() or snd.closing())
    if snd.remote is None:
      snd.close()
      raise LinkError("no such target: %s" % target)
    return snd

  @synchronized
  def receiver(self, source, limit=0, drain=False):
    rcv = Receiver(self.connection, source)
    self.add(rcv)
    rcv.attach()
    if limit:
      rcv.flow(limit, drain=drain)
    self.wait(lambda: rcv.opened() or rcv.closing())
    if rcv.remote is None:
      rcv.close()
      raise LinkError("no such source: %s" % source)
    return rcv

  @synchronized
  def close(self):
    self.end()

class Link:

  def __init__(self, connection):
    self.connection = connection
    self._lock = self.connection._lock
    self.timeout = 120

  def wait(self, predicate, timeout=DEFAULT):
    if timeout is DEFAULT:
      self.timeout = timeout
    self.connection.wait(predicate, timeout)

  @synchronized
  def disposition(self, delivery_tag, state=None, settled=False):
    BaseLink.disposition(self, delivery_tag, state, settled)

  @synchronized
  def settle(self, delivery_tag, state=None):
    BaseLink.settle(self, delivery_tag, state)

  @synchronized
  def close(self):
    self.detach()
    self.wait(self.closed)

class Sender(Link, BaseSender):

  def __init__(self, connection, target):
    BaseSender.__init__(self, str(uuid4()), Linkage(None, target))
    Link.__init__(self, connection)

  @synchronized
  def send(self, **kwargs):
    self.wait(self.capacity)
    return BaseSender.send(self, **kwargs)

class Receiver(Link, BaseReceiver):

  def __init__(self, connection, source):
    BaseReceiver.__init__(self, str(uuid4()), Linkage(source, None))
    Link.__init__(self, connection)

  @synchronized
  def pending(self, block=False, timeout=None):
    if block:
      self.wait(self._pending_unblocked, timeout)
    return BaseReceiver.pending(self)

  def _pending_unblocked(self):
    return self.capacity() == 0 or BaseReceiver.pending(self) > 0

  @synchronized
  def draining(self, block=False, timeout=None):
    if block:
      self.wait(self._draining_unblocked, timeout)
    return BaseReceiver.draining(self)

  def _draining_unblocked(self):
    return BaseReceiver.draining(self)
