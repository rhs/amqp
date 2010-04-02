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
from link import Sender as BaseSender, Receiver as BaseReceiver, LinkError, link
from selector import Selector
from util import ConnectionSelectable
from concurrency import synchronized, Condition, Waiter
from threading import RLock
from uuid import uuid4
from operations import Fragment

class Connection(BaseConnection):

  def __init__(self):
    BaseConnection.__init__(self, self.session)
    self._lock = RLock()
    self.condition = Condition(self._lock)
    self.waiter = Waiter(self.condition)
    self.selector = Selector.default()

  def connect(self, host, port):
    sock = socket.socket()
    sock.connect((host, port))
    sock.setblocking(0)
    self.selector.register(ConnectionSelectable(sock, self, self._tick))

  @synchronized
  def _tick(self, connection):
    connection.tick()
    self.waiter.notify()

  def wait(self, predicate, timeout=10):
    self.selector.wakeup()
    self.waiter.wait(predicate, timeout)

  def session(self, name = None):
    if name is None:
      name = str(uuid4())
    ssn = Session(self, name)
    self.add(ssn)
    ssn.attach()
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

  def wait(self, predicate, timeout=10):
    self.connection.wait(predicate, timeout)

  @synchronized
  def sender(self, target):
    snd = Sender(self.connection, target)
    self.add(snd)
    snd.link()
    self.wait(lambda: snd.opened())
    if snd.target is None:
      snd.close()
      raise LinkError("no such target: %s" % target)
    return snd

  @synchronized
  def receiver(self, source, limit=0):
    rcv = Receiver(self.connection, source)
    self.add(rcv)
    rcv.link()
    if limit:
      rcv.flow(limit)
    self.wait(lambda: rcv.opened())
    if rcv.source is None:
      rcv.close()
      raise LinkError("no such source: %s" % source)
    return rcv

  @synchronized
  def close(self):
    self.detach(True)

class Link:

  def __init__(self, connection):
    self.connection = connection
    self._lock = self.connection._lock

  def wait(self, predicate, timeout=10):
    self.connection.wait(predicate, timeout)

  @synchronized
  def close(self):
    self.unlink()
    self.wait(lambda: self.closed())

class Sender(BaseSender, Link):

  def __init__(self, connection, target):
    BaseSender.__init__(self, str(uuid4()), None, target)
    Link.__init__(self, connection)

  @synchronized
  def send(self, **kwargs):
    self.wait(self.capacity)
    return BaseSender.send(self, **kwargs)

class Receiver(BaseReceiver, Link):

  def __init__(self, connection, source):
    BaseReceiver.__init__(self, str(uuid4()), source, None)
    Link.__init__(self, connection)
