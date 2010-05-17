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
from connection import Connection as ProtoConnection
from session import Session as ProtoSession, SessionError
from link import Link as ProtoLink, Sender as ProtoSender, \
    Receiver as ProtoReceiver, LinkError, link
from selector import Selector
from util import ConnectionSelectable, Constant
from concurrency import synchronized, Condition, Waiter
from threading import RLock
from uuid import uuid4
from protocol import Fragment, Linkage

class Timeout(Exception):
  pass

DEFAULT = Constant("DEFAULT")

class Connection:

  def __init__(self):
    self.proto = ProtoConnection(self.session)
    self._lock = RLock()
    self.condition = Condition(self._lock)
    self.waiter = Waiter(self.condition)
    self.selector = Selector.default()
    self.timeout = 120

  def trace(self, *args, **kwargs):
    self.proto.trace(*args, **kwargs)

  @synchronized
  def connect(self, host, port):
    sock = socket.socket()
    sock.connect((host, port))
    sock.setblocking(0)
    self.selector.register(ConnectionSelectable(sock, self, self.tick))

  @synchronized
  def pending(self):
    return self.proto.pending()

  @synchronized
  def peek(self, n=None):
    return self.proto.peek(n)

  @synchronized
  def read(self, n=None):
    return self.proto.read(n)

  @synchronized
  def write(self, bytes):
    self.proto.write(bytes)

  @synchronized
  def tick(self, connection):
    self.proto.tick()
    self.waiter.notify()

  @synchronized
  def open(self, **kwargs):
    if "container_id" not in kwargs:
      kwargs["container_id"] = str(uuid4())
    if "channel_max" not in kwargs:
      kwargs["channel_max"] = 65535
    self.proto.open(**kwargs)

  def wait(self, predicate, timeout=DEFAULT):
    if timeout is DEFAULT:
      timeout = self.timeout
    self.selector.wakeup()
    if not self.waiter.wait(predicate, timeout):
      raise Timeout()

  @synchronized
  def session(self):
    ssn = Session(self)
    self.proto.add(ssn.proto)
    return ssn

  @synchronized
  def close(self):
    self.proto.close()
    self.wait(lambda: self.proto.close_rcvd)

class Session:

  def __init__(self, connection):
    self.proto = ProtoSession(link)
    self.connection = connection
    self._lock = self.connection._lock
    self.timeout = 120
    self.proto.begin()

  def wait(self, predicate, timeout=DEFAULT):
    if timeout is DEFAULT:
      self.timeout = timeout
    self.connection.wait(predicate, timeout)

  @synchronized
  def sender(self, target):
    snd = Sender(self.connection, target)
    self.proto.add(snd.proto)
    snd.proto.attach()
    self.wait(lambda: snd.proto.opened() or snd.proto.closing())
    if snd.proto.remote is None:
      snd.close()
      raise LinkError("no such target: %s" % target)
    return snd

  @synchronized
  def receiver(self, source, limit=0, drain=False):
    rcv = Receiver(self.connection, source)
    self.proto.add(rcv.proto)
    rcv.proto.attach()
    if limit:
      rcv.flow(limit, drain=drain)
    self.wait(lambda: rcv.proto.opened() or rcv.proto.closing())
    if rcv.proto.remote is None:
      rcv.close()
      raise LinkError("no such source: %s" % source)
    return rcv

  @synchronized
  def close(self):
    self.proto.end()

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
  def get_unsettled(self):
    return dict(self.proto.unsettled)

  @synchronized
  def get_remote(self, *args, **kwargs):
    return self.proto.get_remote(*args, **kwargs)

  @synchronized
  def get_local(self, *args, **kwargs):
    return self.proto.get_local(*args, **kwargs)

  @synchronized
  def capacity(self):
    return self.proto.capacity()

  @synchronized
  def disposition(self, delivery_tag, state=None, settled=False):
    self.proto.disposition(delivery_tag, state, settled)

  @synchronized
  def settle(self, delivery_tag, state=None):
    self.proto.settle(delivery_tag, state)

  @synchronized
  def close(self):
    self.proto.detach()
    self.wait(self.proto.closed)

class Sender(Link):

  def __init__(self, connection, target):
    Link.__init__(self, connection)
    self.proto = ProtoSender(str(uuid4()), Linkage(None, target))

  @synchronized
  def send(self, **kwargs):
    self.wait(self.capacity)
    return self.proto.send(**kwargs)

class Receiver(Link):

  def __init__(self, connection, source):
    Link.__init__(self, connection)
    self.proto = ProtoReceiver(str(uuid4()), Linkage(source, None))

  @synchronized
  def flow(self, limit, drain=False):
    self.proto.flow(limit, drain)

  @synchronized
  def pending(self, block=False, timeout=None):
    if block:
      self.wait(self._pending_unblocked, timeout)
    return self.proto.pending()

  def _pending_unblocked(self):
    return self.capacity() == 0 or self.proto.pending() > 0

  @synchronized
  def draining(self, block=False, timeout=None):
    if block:
      self.wait(self._draining_unblocked, timeout)
    return self.proto.draining()

  def _draining_unblocked(self):
    return self.proto.draining()

  @synchronized
  def get(self):
    return self.proto.get()
