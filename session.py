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

from link import Sender, Receiver
from protocol import Begin, End
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
    getattr(self, "do_%s" % body.NAME, self.unhandled)(body)

  def unhandled(self, body):
    link = self.handles[body.handle]
    link.write(body)

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
                          next_outgoing_id = self.outgoing.unsettled_hwm + 1,
                          incoming_window = 65536, outgoing_window = 65536,
                          properties = self.properties))

  def do_begin(self, begin):
    self.begin_rcvd = True
    self.incoming.unsettled_hwm = begin.next_outgoing_id - 1

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
    if link.handle is not None:
      raise SessionError("link is attached")
    if link.name in self.links and self.links[link.name] == link:
      del self.links[link.name]
      link.session = None
    else:
      raise SessionError("no such link")

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

  def do_detach(self, detach):
    if detach.handle not in self.handles:
      raise SessionError("double detach")

    link = self.handles.pop(detach.handle)
    link.write(detach)

  def do_disposition(self, disp):
    role = self.roles[not disp.role]
    start = max(role.unsettled_lwm, disp.first)
    if disp.last is None:
      last = start + 1
    else:
      last = disp.last + 1
    for id in range(start, last):
      delivery = role.get_delivery(id)
      if delivery:
        link, tag = delivery
        link.do_disposition(tag, disp.state, disp.settled)

  def tick(self):
    for link in self.links.values():
      link.tick()

class DeliveryMap:

  def __init__(self):
    # (link, delivery_tag) -> ranges
    self.transfers = {}
    # (transfer_id - unsettled_lwm) -> (link, delivery_tag)
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
