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
from connection import Connection
from sasl import SASL
from session import Session, SessionError, FIXED
from link import LinkError, Receiver, Sender, link
from util import ConnectionSelectable
from protocol import Target, Coordinator, Declare, Declared, Discharge, \
    TransactionalState, ACCEPTED
from messaging import decode

class Transaction:

  def __init__(self, txn_id):
    self.txn_id = txn_id
    self.work = []

  def add_work(self, doit, undo):
    self.work.append((doit, undo))

  def discharge(self, fail):
    for doit, undo in self.work:
      if fail:
        undo()
      else:
        doit()

class TxnCoordinator:

  def __init__(self):
    self.transactions = {}
    self.next_id = 0

  def declare(self):
    id = self.next_id
    self.next_id += 1
    return id

  def discharge(self, txn_id, fail):
    if txn_id in self.transactions:
      txn = self.transactions.pop(txn_id)
      txn.discharge(fail)

  def get_transaction(self, state):
    if isinstance(state, TransactionalState):
      txn_id = state.txn_id
      if txn_id in self.transactions:
        return self.transactions[txn_id]
      else:
        txn = Transaction(txn_id)
        self.transactions[txn_id] = txn
        return txn

  def target(self):
    return TxnTarget(self)

class TxnTarget:

  def __init__(self, coordinator):
    self.coordinator = coordinator
    self.dispatch = {Declare: self.declare, Discharge: self.discharge}
    self.unsettled = {}

  def capacity(self):
    return True

  def put(self, dtag, xfr):
    msg = decode(xfr)
    self.dispatch[msg.content.__class__](xfr.state, msg)

  def declare(self, state, msg):
    txn_id = self.coordinator.declare()
    self.unsettled[msg.delivery_tag] = Declared(txn_id)

  def discharge(self, state, msg):
    self.coordinator.discharge(msg.content.txn_id, msg.content.fail)
    self.unsettled[msg.delivery_tag] = ACCEPTED

  def settle(self, dtag):
    return self.unsettled.pop(dtag)

  def orphaned(self):
    return True

  def close(self):
    pass

class Broker:

  def __init__(self, container_id):
    self.container_id = container_id
    self.window = 65536
    self.period = None
    self.frame_size = 4294967295
    self.auth = False
    self.mechanisms = ()
    self.passwords = {}
    self.traces = ()
    self.coordinator = TxnCoordinator()
    self.nodes = {}
    self.sources = {}
    self.targets = {}

    self.attach = {Sender.role: self.attach_sender,
                   Receiver.role: self.attach_receiver}
    self.process = {Sender.role: self.process_sender,
                    Receiver.role: self.process_receiver}
    self.orphan = {Sender.role: self.orphan_sender,
                   Receiver.role: self.orphan_receiver}
    self.detach = {Sender.role: self.detach_sender,
                   Receiver.role: self.detach_receiver}
    self.resolvers = {Target: self.resolve_target,
                      Coordinator: self.resolve_coordinator}

    self.sock = None
    self.listener = None

  def bind(self, host, port):
    self.sock = socket.socket()
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.sock.bind((host, port))
    self.sock.listen(5)
    self.sock.setblocking(0)

  def fileno(self):
    return self.sock.fileno()

  def reading(self):
    return True

  def writing(self):
    return False

  def timing(self):
    return None

  def readable(self, selector):
    sock, addr = self.sock.accept()
    conn = Connection(lambda properties: Session(link, properties))
    conn.tracing(*self.traces)
    if self.auth:
      sasl = SASL(conn)
      sasl.tracing(*self.traces)
      sasl.server(self.mechanisms, passwords=self.passwords)
      sel = sasl
    else:
      sel = conn
    selector.register(ConnectionSelectable(sock, sel, self.tick, self.period,
                                           self.timeout))

  def timeout(self, connection):
    for ssn in connection.incoming.values() + connection.outgoing.values():
      ssn.set_incoming_window(self.window, FIXED)

  def tick(self, connection):
    if self.auth:
      self.sasl_tick(connection)
    else:
      self.amqp_tick(connection)
    if self.listener:
      self.listener()

  def sasl_tick(self, sasl):
    sasl.tick()
    if sasl.outcome != 0:
      return
    else:
      self.amqp_tick(sasl.connection)

  def amqp_tick(self, connection):
    if connection.opening():
      connection.open(container_id = self.container_id,
                      channel_max = 65535, max_frame_size=self.frame_size)

    # XXX
    for ssn in connection.incoming.values():
      if ssn.beginning():
        ssn.begin()
        if self.period:
          ssn.set_incoming_window(self.window, FIXED)
        else:
          ssn.set_incoming_window(self.window)

    for ssn in connection.outgoing.values():
      if ssn.beginning():
        ssn.begin()
        if self.period:
          ssn.set_incoming_window(self.window, FIXED)
        else:
          ssn.set_incoming_window(self.window)

      for link in ssn.links.values():
        if link.attaching():
          if self.attach[link.role](link):
            link.attach()
          else:
            # XXX
            link.modified = True
            link.attach()
            link.detach()

        self.process[link.role](link)

        if link.detaching():
          self.detach[link.role](link)
          link.detach()
        elif ssn.ending():
          self.orphan[link.role](link)

        if link.detached():
          ssn.remove(link)

      if ssn.ending():
        ssn.end()
        connection.remove(ssn)

    if connection.closing():
      connection.close()

    connection.tick()

  def attach_sender(self, link):
    if link.name in self.sources:
      source = self.sources[link.name]
      source.resume(link.unsettled or {})
      # XXX: should actually set this to reflect the real source and
      # possibly update the real source
      link.source = link.remote_source
      link.target = link.remote_target
      return True
    elif link.remote_source.address in self.nodes:
      n = self.nodes[link.remote_source.address]
      source = n.source()
      self.sources[link.name] = source
      link.source = link.remote_source
      link.target = link.remote_target
      return True
    else:
      return False

  def attach_receiver(self, link):
    if link.name in self.targets:
      target = self.targets[link.name]
      target.resume(link.unsettled or {})
      link.source = link.remote_source
      # XXX: should actually set this to reflect the real target and
      # possibly update the real target
      link.target = link.remote_target
      return True
    else:
      n = self.resolve(link.remote_target)
      if n is None:
        return False
      else:
        target = n.target()
        self.targets[link.name] = target
        if target.capacity():
          link.flow(20)
        link.source = link.remote_source
        link.target = link.remote_target
        return True

  def resolve(self, target):
    return self.resolvers[target.__class__](target)

  def resolve_target(self, target):
    return self.nodes.get(target.address)

  def resolve_coordinator(self, target):
    return self.coordinator

  def process_sender(self, link):
    if link.source is None: return
    source = self.sources[link.name]
    while link.capacity() > 0:
      tag, xfr = source.get()
      if xfr is None:
        link.drained()
        break
      else:
        link.send(delivery_tag = tag, message_format = xfr.message_format,
                  settled = link.snd_settle_mode == 1, # XXX: enums
                  payload = xfr.payload)

    for t, _, r in link.get_remote(modified=True):
      if r.settled or r.state is not None:
        def doit(t=t, s=r.state):
          state = source.settle(t, r.state)
          link.settle(t, state)
        def undo(t=t):
          state = source.settle(t, None)
          link.settle(t, state)
        if r.state:
          txn = self.coordinator.get_transaction(r.state)
        else:
          txn = None
        if txn:
          txn.add_work(doit, undo)
        else:
          doit()
      r.modified = False

  def process_receiver(self, link):
    if link.target is None: return
    target = self.targets[link.name]
    while link.pending():
      xfr = link.get()
      if not isinstance(target, TxnTarget):
        if xfr.state:
          txn = self.coordinator.get_transaction(xfr.state)
        else:
          txn = None
      else:
        txn = None
      entry = target.put(xfr.delivery_tag, xfr)
      if txn:
        entry.acquire(txn)
        # XXX: consider using settlement rather than entry
        txn.add_work(lambda: entry.release(), lambda: entry.remove())
      link.disposition(xfr.delivery_tag, ACCEPTED)
      # XXX: enums
      if link.rcv_settle_mode == 0:
        link.settle(xfr.delivery_tag)

    for t, _, r in link.get_remote():
      if r.settled:
        state = target.settle(t)
        link.settle(t, state)

    if target.capacity() and link.credit() < 10: link.flow(10)

  def orphan_sender(self, link):
    source = self.sources[link.name]
    if source.orphaned():
      del self.sources[link.name]

  def orphan_receiver(self, link):
    target = self.targets[link.name]
    if target.orphaned():
      del self.targets[link.name]

  def detach_sender(self, link):
    if link.source and link.remote_source is None:
      source = self.sources.pop(link.name)
      source.close()
      link.source = link.remote_source
      link.target = link.remote_target

  def detach_receiver(self, link):
    if link.target and link.remote_target is None:
      target = self.targets.pop(link.name)
      target.close()
      link.source = link.remote_source
      link.target = link.remote_target
