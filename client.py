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
from session import Session as ProtoSession, SessionError, FIXED, SLIDING
from link import Link as ProtoLink, Sender as ProtoSender, \
    Receiver as ProtoReceiver, LinkError, link
from messaging import Message, encode, decode
from selector import Selector
from util import ConnectionSelectable, Constant
from concurrency import synchronized, Condition, Waiter
from threading import RLock
from uuid import uuid4
from protocol import Source, Target, ACCEPTED

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

  def tracing(self, *args, **kwargs):
    self.proto.tracing(*args, **kwargs)

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
  def sender(self, target, name=None):
    snd = Sender(self.connection, name or str(uuid4()), Target(address=target))
    self.proto.add(snd.proto)
    snd.proto.attach()
    self.wait(lambda: snd.proto.opened() or snd.proto.closing())
    if snd.proto.target is None:
      snd.close()
      raise LinkError("no such target: %s" % target)
    return snd

  @synchronized
  def receiver(self, source, limit=0, drain=False, name=None):
    rcv = Receiver(self.connection, name or str(uuid4()),
                   Source(address=source))
    self.proto.add(rcv.proto)
    if limit:
      rcv.flow(limit, drain=drain)
    rcv.proto.attach()
    self.wait(lambda: rcv.proto.opened() or rcv.proto.closing())
    if rcv.proto.source is None:
      rcv.close()
      raise LinkError("no such source: %s" % source)
    return rcv

  @synchronized
  def incoming_window(self):
    return self.proto.incoming_window(self)

  @synchronized
  def set_incoming_window(self, *args, **kwargs):
    return self.proto.set_incoming_window(*args, **kwargs)

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
  def credit(self):
    return self.proto.credit()

  @synchronized
  def disposition(self, delivery_tag, state=None, settled=False):
    self.proto.disposition(delivery_tag, state, settled)

  @synchronized
  def settle(self, delivery_tag, state=None):
    self.proto.settle(delivery_tag, state)

  @synchronized
  def detach(self):
    self.proto.detach()
    # XXX
    self.wait(self.proto.closed)

  @synchronized
  def close(self):
    self.proto.close()
    self.wait(self.proto.closed)

class Sender(Link):

  def __init__(self, connection, name, target):
    Link.__init__(self, connection)
    self.proto = ProtoSender(name, None, target)

  @synchronized
  def send(self, message=None, delivery_tag=None, **kwargs):
    self.wait(self.capacity)
    if message:
      kwargs["fragments"] = encode(message, self.connection.proto.type_encoder)
    return self.proto.send(delivery_tag=delivery_tag, **kwargs)

class Receiver(Link):

  def __init__(self, connection, name, source):
    Link.__init__(self, connection)
    self.proto = ProtoReceiver(name, source, None)

  @synchronized
  def flow(self, limit, drain=False):
    self.proto.flow(limit, drain)

  @synchronized
  def pending(self, block=False, timeout=None):
    if block:
      self.wait(self._pending_unblocked, timeout)
    return self.proto.pending()

  def _pending_unblocked(self):
    return self.credit() == 0 or self.proto.pending() > 0

  @synchronized
  def draining(self, block=False, timeout=None):
    if block:
      self.wait(self._draining_unblocked, timeout)
    return self.proto.draining()

  def _draining_unblocked(self):
    return self.proto.draining()

  @synchronized
  def get(self):
    return decode(self.proto.get(), self.connection.proto.type_decoder)
