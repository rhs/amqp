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
from protocol import Begin, End, Flow
from util import RangeSet, Constant

SLIDING = Constant("SLIDING")
FIXED = Constant("FIXED")

class SessionError(Exception):
  pass

class Session:

  def __init__(self, factory, properties=None):
    self.factory = factory
    self.properties = properties
    self.remote_channel = None
    self.channel = None
    self.max_frame_size = None

    self.begin_sent = False
    self.begin_rcvd = False
    self.end_sent = False
    self.end_rcvd = False

    self.incoming = Incoming()
    self.outgoing = Outgoing()
    self.next_receiver_id = None
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
                          incoming_window = self.incoming.window,
                          outgoing_window = 65536, # this should NOT be self.outgoing.window
                          handle_max = 2147483647,
                          properties = self.properties))

  def do_begin(self, begin):
    self.begin_rcvd = True
    self.incoming.transfer_count = begin.next_outgoing_id - 1
    self.outgoing.max_id = self.outgoing.transfer_count + begin.incoming_window - 1

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

  def do_flow(self, flow):
    if flow.next_incoming_id is None:
      start = Outgoing.initial_transfer
    else:
      start = flow.next_incoming_id
    self.outgoing.window = start + flow.incoming_window - self.outgoing.unsettled_hwm - 1
    if flow.handle is not None:
      link = self.handles[flow.handle]
      link.write(flow)

  def incoming_window(self):
    return self.incoming.window

  def set_incoming_window(self, window, policy=SLIDING):
    old = self.incoming.window
    self.incoming.window = window
    self.incoming.policy = policy
    if window != old and self.begin_sent and not self.end_sent:
      if self.incoming.unsettled_hwm is None:
        next = None
      else:
        next = self.incoming.unsettled_hwm + 1
      self.post_frame(Flow(next_outgoing_id = self.outgoing.unsettled_hwm + 1,
                           outgoing_window = self.outgoing.window,
                           next_incoming_id = next,
                           incoming_window = self.incoming.window))

  def tick(self):
    for link in self.links.values():
      link.tick()

  def update_next_receiver(self, xfr):
    if xfr.delivery_id == self.next_receiver_id:
      self.next_receiver_id += 1

  def next_receiver(self):
    if self.next_receiver_id is None or self.next_receiver_id > self.incoming.unsettled_hwm:
      return None

    while True:
      d = self.incoming.get_delivery(self.next_receiver_id)
      if d:
        return d[0]
      elif self.next_receiver_id < self.unsettled_hwm:
        self.next_receiver_id += 1
      else:
        return None

class DeliveryMap:

  def __init__(self):
    # (link, delivery_tag) -> delivery_id
    self.aliases = {}
    # (delivery_id - unsettled_lwm) -> (link, delivery_tag)
    self.deliveries = []
    # lowest unsettled delivery_id
    self.unsettled_lwm = None
    # highest unsettled delivery_id
    self.unsettled_hwm = None

    self.transfer_count = None
    self.window = None
    self.policy = SLIDING
    self.init()

  def capacity(self):
    return self.window

  def append(self, link, transfer):
    self.window -= 1
    delivery = (link, transfer.delivery_tag)
    if delivery in self.aliases:
      # XXX
      transfer.delivery_id = self.unsettled_hwm
    else:
      self.mark(transfer)
      self.aliases[delivery] = transfer.delivery_id
      self.deliveries.append(delivery)
    self.transfer_count += 1

  def settle(self, link, delivery_tag):
    delivery = (link, delivery_tag)
    id = self.aliases.pop(delivery)
    idx = id - self.unsettled_lwm
    assert self.deliveries[idx] == delivery
    self.deliveries[idx] = None

    while self.deliveries:
      if self.deliveries[0] is None:
        self.deliveries.pop(0)
        self.unsettled_lwm += 1
      else:
        break

  def get_delivery(self, delivery_id):
    return self.deliveries[delivery_id - self.unsettled_lwm]

  def __repr__(self):
    return "%s(%r, %r, %s, %s)" % (self.__class__, self.aliases, self.deliveries,
                                   self.unsettled_lwm, self.unsettled_hwm)

class Incoming(DeliveryMap):

  def init(self):
    self.window = 65536

  def append(self, link, transfer):
    DeliveryMap.append(self, link, transfer)
    if self.policy is SLIDING:
      self.window += 1

  def mark(self, transfer):
    if self.unsettled_lwm is None:
      self.unsettled_lwm = transfer.delivery_id
    else:
      assert transfer.delivery_id == self.unsettled_hwm + 1
    self.unsettled_hwm = transfer.delivery_id

class Outgoing(DeliveryMap):

  initial_delivery = 1
  initial_transfer = 1

  def init(self):
    self.unsettled_lwm = self.initial_delivery
    self.unsettled_hwm = self.initial_delivery - 1
    self.transfer_count = self.initial_transfer
    self.window = 0

  def mark(self, transfer):
    assert transfer.delivery_id is None
    self.unsettled_hwm += 1
    transfer.delivery_id = self.unsettled_hwm
