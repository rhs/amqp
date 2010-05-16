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

from link import ATTACHED, DETACHED, Sender, Receiver
from protocol import Begin, Attach, Flow, Transfer, Disposition, Extent, \
    Detach, End
from util import RangeSet

class SessionError(Exception):
  pass

class Session:

  def __init__(self, factory, properties=None):
    self.factory = factory
    self.properties = properties
    self.remote_channel = None
    self.channel = None

    self.begin_sent = False
    self.begin_rcvd = False
    self.end_sent = False
    self.end_rcvd = False

    self.incoming = Incoming()
    self.outgoing = Outgoing()
    self.roles = {Sender.role: self.outgoing,
                  Receiver.role: self.incoming}

    # link name -> link endpoint
    self.links = {}
    # handle -> link endpoint
    self.handles = {}

    self.output = []

  def write(self, body):
    self.dispatch(body)

  # XXX: dup of read in framing
  def read(self, n=None):
    result = self.output[:n]
    del self.output[:n]
    return result

  def dispatch(self, body):
    getattr(self, "do_%s" % body.NAME)(body)

  def post_frame(self, body):
    assert self.begin_sent and not self.end_sent
    self.output.append(body)

  def beginning(self):
    return self.begin_rcvd and not self.begin_sent

  def ending(self):
    return self.end_rcvd and not self.end_sent

  def begin(self):
    if self.begin_sent:
      raise SessionError("already begun")
    self.begin_sent = True
    self.post_frame(Begin(remote_channel = self.remote_channel,
                          properties = self.properties))

  def do_begin(self, begin):
    self.begin_rcvd = True

  def end(self, error=None):
    if self.end_sent:
      raise SessionError("already ended")
    # process any outstanding work before ending
    self.tick()
    self.post_frame(End())
    self.end_sent = True

  def do_end(self, det):
    self.end_rcvd = True

  def add(self, link):
    link.session = self
    self.links[link.name] = link

  def allocate_handle(self):
    return max([-1] + [l.handle for l in self.links.values()]) + 1

  def remove(self, link):
    # process any outstanding work before removing
    self.tick()
    link.session = None
    link.handle = None
    del self.links[link.name]

  def do_attach(self, attach):
    if attach.handle in self.handles:
      raise SessionError("double attach")

    if self.links.has_key(attach.name):
      link = self.links[attach.name]
    else:
      link = self.factory(attach)
      self.add(link)

    self.handles[attach.handle] = link
    link.write(attach)

  def do_flow(self, flow):
    link = self.handles[flow.handle]
    link.do_flow(flow.flow_state)

  def do_transfer(self, xfr):
    link = self.handles[xfr.handle]
    self.incoming.append(link, xfr)
    link.write(xfr)

  def do_disposition(self, disp):
    if disp.extents:
      role = self.roles[not disp.role]
      for e in disp.extents:
        start = max(role.unsettled_lwm, e.first)
        for id in range(start, e.last+1):
          delivery = role.get_delivery(id)
          if delivery:
            link, tag = delivery
            link.do_disposition(tag, e.state, e.settled)

  def do_detach(self, detach):
    if detach.handle not in self.handles:
      raise SessionError("double detach")

    link = self.handles.pop(detach.handle)
    link.write(detach)

  def tick(self):
    for link in self.links.values():
      if (link.modified or link.local_state is ATTACHED) and link.handle is None:
        link.handle = self.allocate_handle()
        self.post_frame(Attach(name = link.name,
                               handle = link.handle,
                               flow_state = self.flow_state(link),
                               role = link.role,
                               local = link.local,
                               remote = link.remote))
        link.modified = False

      if link.handle is not None:
        self.process_link(link)

        if link.local_state is DETACHED:
          self.post_frame(Detach(handle=link.handle))
          link.handle = None

  def flow_state(self, link):
    state = link.flow_state()
    self.session_flow(state)
    return state

  def session_flow(self, state):
    state.unsettled_lwm = self.incoming.unsettled_lwm
    state.session_credit = 65536

  def process_link(self, l):
    # XXX
    if l.role == Sender.role:
      while l.outgoing:
        xfr = l.outgoing.pop(0)
        xfr.handle = l.handle
        self.session_flow(xfr.flow_state)
        self.outgoing.append(l, xfr)
        self.post_frame(xfr)

    # we don't send flow state until the transfer_count is
    # initialized, this ensures an unambiguous calculation of the
    # initial transfer_count
    if l.modified and l.transfer_count is not None:
      self.post_frame(Flow(handle=l.handle, flow_state=self.flow_state(l)))
      l.modified = False

    states = {}

    for dtag, local in l.get_local(modified=True):
      role = self.roles[l.role]
      ids = role.transfers[(l, dtag)]
      if local in states:
        ranges = states[local]
      else:
        ranges = RangeSet()
        states[local] = ranges
      for r in ids:
        ranges.add_range(r)

      if local.settled:
        role.settle(l, dtag)
        l.unsettled.pop(dtag)
      local.modified = False

    for local, ranges in states.items():
      extents = []
      for r in ranges:
        ext = Extent(r.lower, r.upper, settled=local.settled, state=local.state)
        extents.append(ext)
      self.post_frame(Disposition(role=l.role, extents=extents))

class DeliveryMap:

  def __init__(self):
    # (link, delivery_tag) -> ranges
    self.transfers = {}
    # transfer_id -> (link, delivery_tag)
    self.deliveries = []
    # lowest unsettled transfer_id
    self.unsettled_lwm = None
    # highest unsettled transfer_id
    self.unsettled_hwm = None
    self.capacity = None
    self.init()

  def append(self, link, transfer):
    delivery = (link, transfer.delivery_tag)
    self.mark(transfer)
    id = transfer.transfer_id
    if delivery in self.transfers:
      ranges = self.transfers[delivery]
    else:
      ranges = RangeSet()
      self.transfers[delivery] = ranges
    ranges.add(id)
    self.deliveries.append(delivery)

  def settle(self, link, delivery_tag):
    delivery = (link, delivery_tag)
    ranges = self.transfers.pop(delivery)
    for r in ranges:
      for id in r:
        idx = id - self.unsettled_lwm
        assert self.deliveries[idx] == delivery
        self.deliveries[idx] = None

    while self.deliveries:
      if self.deliveries[0] is None:
        self.deliveries.pop(0)
        self.unsettled_lwm += 1
      else:
        break

  def get_delivery(self, transfer_id):
    return self.deliveries[transfer_id - self.unsettled_lwm]

  def __repr__(self):
    return "%s(%r, %r, %s, %s)" % (self.__class__, self.transfers, self.deliveries,
                                   self.unsettled_lwm, self.unsettled_hwm)

class Incoming(DeliveryMap):

  def init(self):
    pass

  def mark(self, transfer):
    if self.unsettled_lwm is None:
      self.unsettled_lwm = transfer.transfer_id
    else:
      assert transfer.transfer_id == self.unsettled_hwm + 1
    self.unsettled_hwm = transfer.transfer_id

class Outgoing(DeliveryMap):

  def init(self):
    self.unsettled_lwm = 1
    self.unsettled_hwm = 0

  def mark(self, transfer):
    assert transfer.transfer_id is None
    self.unsettled_hwm += 1
    transfer.transfer_id = self.unsettled_hwm
