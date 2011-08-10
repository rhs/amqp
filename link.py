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

from protocol import Attach, Flow, Transfer, Disposition, Detach, Binary
from util import Constant, RangeSet
from uuid import uuid4

class LinkError(Exception):
  pass

class State:

  def __init__(self, state=None, settled=False, modified=False, resumed=False):
    self.state = state
    self.settled = settled
    self.modified = modified
    self.resumed = resumed

  def __hash__(self):
    return hash(self.state) ^ hash(self.settled)

  def __eq__(self, o):
    return self.state == o.state and self.settled == o.settled

  def __repr__(self):
    return "State(%s, %s, %s)" % (self.state, self.settled, self.modified)

class Link(object):

  def __init__(self, name, source=None, target=None):
    self.name = name
    self.source = source
    self.target = target
    self.remote_source = None
    self.remote_target = None
    self.snd_settle_mode = None
    self.rcv_settle_mode = None

    self.session = None
    self.handle = None

    self.attach_sent = False
    self.attach_rcvd = False
    self.detach_sent = False
    self.detach_rcvd = False

    # flow state
    self.delivery_count = None
    self.link_credit = 0
    self.available = 0
    self.drain = False
    self.echo = False

    # delivery-tag -> (local_state, remote_state)
    self.unsettled = {}

    self.init()

  def capacity(self):
    return min(self.link_credit, self.session.roles[self.role].capacity())

  def credit(self):
    return self.link_credit

  def attaching(self):
    return self.attach_rcvd and not self.attach_sent

  def detaching(self):
    return self.detach_rcvd and not self.detach_sent

  def attached(self):
    return self.attach_sent and self.attach_rcvd

  def detached(self):
    return self.detach_sent and self.detach_rcvd

  def write(self, body):
    self.dispatch(body)

  def dispatch(self, body):
    return getattr(self, "do_%s" % body.NAME)(body)

  def post_frame(self, body):
    assert self.attach_sent and not self.detach_sent
    body.handle = self.handle
    self.session.post_frame(body)

  def _flow(self):
    if self.session.incoming.unsettled_hwm is None:
      next = None
    else:
      next = self.session.incoming.unsettled_hwm + 1
    flow = Flow(handle = self.handle,
                next_incoming_id = next,
                incoming_window = self.session.incoming.window,
                next_outgoing_id = self.session.outgoing.unsettled_hwm + 1,
                outgoing_window = 65536,
                delivery_count = self.delivery_count,
                link_credit = self.link_credit,
                available = self.available,
                drain = self.drain)
    self.echo = False
    return flow

  def attach(self):
    if self.attach_sent:
      raise LinkError("already attached")
    self.handle = self.session.allocate_handle()
    self.attach_sent = True
    unsettled = {}
    for tag, local, remote in self.get_local(settled=False):
      if not local.resumed:
        unsettled[Binary(tag)] = local.state
    self.post_frame(Attach(name = self.name,
                           role = self.role,
                           source = self.source,
                           target = self.target,
                           initial_delivery_count = self.delivery_count,
                           unsettled = unsettled))
    if self.role == Receiver.role:
      self.post_frame(self._flow())

  def do_attach(self, attach):
    self.attach_rcvd = True
    if self.role == Receiver.role:
      self.delivery_count = attach.initial_delivery_count
    self.remote_source = attach.source
    self.remote_target = attach.target
    self.snd_settle_mode = attach.snd_settle_mode
    self.rcv_settle_mode = attach.rcv_settle_mode
    if attach.unsettled:
      for tag, state in attach.unsettled.items():
        if tag in self.unsettled:
          local, remote = self.unsettled[tag]
          remote.state = state
          remote.modified = True
        else:
          self.unsettled[tag] = State(resumed=True), State(state, modified=True)

  # XXX: closing and errors
  def detach(self, closed=False):
    if self.detach_sent:
      raise LinkError("not attached")
    # process any outstanding work before detaching
    self.tick()
    self.post_frame(Detach(closed=closed))
    self.detach_sent = True
    self.handle = None

  def close(self):
    self.source = None
    self.target = None
    self.detach(True)

  def do_detach(self, detach):
    self.detach_rcvd = True
    if detach.closed:
      self.remote_source = None
      self.remote_target = None

  def do_disposition(self, delivery_tag, state, settled):
    if delivery_tag in self.unsettled:
      local, remote = self.unsettled[delivery_tag]
      remote.state = state
      remote.settled = settled
      remote.modified = True

  def do_flow(self, flow):
    self.do_flow_state(flow)
    self.echo = self.echo or flow.echo

  def _query(self, index, settled=None, modified=None):
    return [(delivery_tag, pair[0], pair[1])
            for delivery_tag, pair in self.unsettled.items()
            if (settled is None or settled == pair[index].settled) and
            (modified is None or modified == pair[index].modified)]

  def get_local(self, settled=None, modified=None):
    return self._query(0, settled, modified)

  def get_remote(self, settled=None, modified=None):
    return [(t, l, r) for t, l, r in self._query(1, settled, modified) if not l.settled]

  def remote_unsettled(self):
    unsettled = {}
    for tag, local, remote in self.get_remote(modified=True):
      unsettled[tag] = remote.state
    return unsettled

  def resume(self, delivery_tag, state):
    if delivery_tag in self.unsettled:
      local, remote = self.unsettled[delivery_tag]
      local.state = state
      local.modified = True
      local.resumed = False
    else:
      self.unsettled[delivery_tag] = (State(state), State())

  def disposition(self, delivery_tag, state=None, settled=False):
    local, remote = self.unsettled[delivery_tag]
    local.state = state
    local.settled = settled
    local.modified = True
    # XXX
    if local.settled and self.handle is None:
      self.unsettled.pop(delivery_tag)
    return local, remote

  def settle(self, delivery_tag, state=None):
    if state is None:
      local, _ = self.unsettled[delivery_tag]
      state = local.state
    return self.disposition(delivery_tag, state, settled=True)

  def tick(self):
    if self.handle is None:
      return

    # we don't send flow state until the delivery_count is
    # initialized, this ensures an unambiguous calculation of the
    # initial delivery_count
    if self.echo and self.delivery_count is not None:
      self.post_frame(self._flow())

    role = self.session.roles[self.role]
    states = {}

    for dtag, local, remote in self.get_local(modified=True):
      if not remote.settled:
        id = role.aliases.get((self, dtag))
        if id is not None:
          if local in states:
            ranges = states[local]
          else:
            ranges = RangeSet()
            states[local] = ranges
          ranges.add(id)
        local.modified = False

      if local.settled:
        if (self, dtag) in role.aliases:
          role.settle(self, dtag)
        self.unsettled.pop(dtag)

    for local, ranges in states.items():
      for r in ranges:
        disp = Disposition(role=self.role, first=r.lower, last=r.upper,
                           settled=local.settled, state=local.state)
        self.session.post_frame(disp)

class Sender(Link):

  # XXX
  role = False
  initial_count = 0

  def init(self):
    self.delivery_count = self.initial_count

  def do_flow_state(self, state):
    if state.delivery_count is None:
      receiver_count = self.initial_count
    else:
      receiver_count = state.delivery_count
    self.link_credit = receiver_count + state.link_credit - self.delivery_count
    self.drain = state.drain

  def drained(self):
    if self.drain:
      self.delivery_count += self.link_credit
      self.link_credit = 0
      self.post_frame(self._flow())

  def send(self, **kwargs):
    if self.link_credit <= 0:
      raise LinkError("would block")
    if kwargs.get("delivery_tag") is None:
      kwargs["delivery_tag"] = "%s" % self.delivery_count or 0

    delivery_tag = kwargs.get("delivery_tag")
    state = kwargs.get("state")
    settled = kwargs.get("settled")

    self.delivery_count += 1
    self.link_credit -= 1

    if delivery_tag in self.unsettled:
      local, remote = self.unsettled[delivery_tag]
      if remote.state is not None:
        kwargs["resume"] = True

    xfrs = self.fragment(**kwargs)

    for xfr in xfrs:
      self.session.outgoing.append(self, xfr)
      self.post_frame(xfr)

    if settled:
      self.session.outgoing.settle(self, delivery_tag)
    elif delivery_tag not in self.unsettled:
      self.unsettled[delivery_tag] = (State(), State(state))

    return delivery_tag

  def xfr_overhead(self, xfr):
    return 192 + len(xfr.delivery_tag or "")

  def fragment(self, **kwargs):
    payload = kwargs.pop("payload", "")
    result = []
    while True:
      xfr = Transfer(**kwargs)
      max_size = self.session.max_frame_size - self.xfr_overhead(xfr)
      xfr.payload = payload[:max_size]
      payload = payload[max_size:]
      if result: result[-1].more = True
      result.append(xfr)
      if not payload:
        break
    return result

class Receiver(Link):

  # XXX
  role = True

  def init(self):
    self.incoming = []
    self.tag = None
    self.payloads = []

  def do_transfer(self, xfr):
    self.session.incoming.append(self, xfr)
    if self.session.next_receiver_id is None:
      self.session.next_receiver_id = xfr.delivery_id
    if self.tag is None:
      self.tag = xfr.delivery_tag
    elif self.tag != xfr.delivery_tag:
      raise ValueError("mismatched tags: %s, %s" % (self.tag, xfr.delivery_tag))

    if xfr.payload:
      self.payloads.extend(xfr.payload)

    if not xfr.more:
      self.tag = None
      payload = "".join(self.payloads)
      self.tag = None
      self.payloads = []
      xfr.payload = payload
      self.incoming.append(xfr)
      if xfr.delivery_tag in self.unsettled:
        local, remote = self.unsettled[xfr.delivery_tag]
        if xfr.state:
          remote.state = xfr.state
        remote.modified = True
      else:
        self.unsettled[xfr.delivery_tag] = (State(), State(xfr.state, xfr.settled, modified=True))
      self.link_credit -= 1
      self.delivery_count += 1
      self.available = max(0, self.available - 1)

  def do_flow_state(self, state):
    if self.delivery_count is not None:
      self.link_credit -= state.delivery_count - self.delivery_count
    self.delivery_count = state.delivery_count
    self.available = state.available

  def flow(self, n, drain=False):
    self.link_credit += n
    self.drain = drain
    self.echo = True

  def drain(self):
    self.flow(0, True)

  def pending(self):
    return len(self.incoming)

  def get(self):
    if self.incoming:
      xfr = self.incoming.pop(0)
      self.session.update_next_receiver(xfr)
      return xfr
    else:
      raise LinkError("empty")

ROLES = {
  Sender.role: Sender,
  Receiver.role: Receiver
  }

def link(attach):
  cls = ROLES[not attach.role]
  return cls(attach.name)
