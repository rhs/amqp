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

from protocol import Attach, Flow, FlowState, Transfer, Disposition, Extent, \
    Detach
from util import Constant, RangeSet
from uuid import uuid4

class LinkError(Exception):
  pass

class State:

  def __init__(self, state=None, settled=False, modified=False):
    self.state = state
    self.settled = settled
    self.modified = modified

  def __hash__(self):
    return hash(self.state) ^ hash(self.settled)

  def __eq__(self, o):
    return self.state == o.state and self.settled == o.settled

  def __repr__(self):
    return "State(%s, %s, %s)" % (self.state, self.settled, self.modified)

class Link(object):

  def __init__(self, name, local, remote=None):
    self.name = name
    self.local = local
    self.remote = remote

    self.session = None
    self.handle = None

    self.attach_sent = False
    self.attach_rcvd = False
    self.detach_sent = False
    self.detach_rcvd = False

    # flow state
    self.transfer_count = None
    self.link_credit = 0
    self.available = 0
    self.drain = False
    self.echo = False

    # used to provide default delivery-tag
    self.delivery_count = 0

    # delivery-tag -> (local_state, remote_state)
    self.unsettled = {}

    self.init()

  def capacity(self):
    return self.link_credit

  # XXX: should update the naming
  def opening(self):
    return self.attach_rcvd and not self.attach_sent

  def closing(self):
    return self.detach_rcvd and not self.detach_sent

  def opened(self):
    return self.attach_sent and self.attach_rcvd

  def closed(self):
    return self.detach_sent and self.detach_rcvd

  def write(self, body):
    self.dispatch(body)

  def dispatch(self, body):
    return getattr(self, "do_%s" % body.NAME)(body)

  def post_frame(self, body):
    assert self.attach_sent and not self.detach_sent
    body.handle = self.handle
    self.session.post_frame(body)

  def flow_state(self):
    state = FlowState(unsettled_lwm = self.session.roles[self.role].unsettled_lwm,
                      session_credit = 65536,
                      transfer_count = self.transfer_count,
                      link_credit = self.link_credit,
                      available = self.available,
                      drain = self.drain)
    self.echo = False
    return state

  def attach(self):
    if self.attach_sent:
      raise LinkError("already attached")
    self.handle = self.session.allocate_handle()
    self.attach_sent = True
    self.post_frame(Attach(name = self.name,
                           flow_state = self.flow_state(),
                           role = self.role,
                           local = self.local,
                           remote = self.remote))

  def do_attach(self, attach):
    self.attach_rcvd = True
    self.remote = attach.local
    self.do_flow_state(attach.flow_state)

  # XXX: closing and errors
  def detach(self):
    if self.detach_sent:
      raise LinkError("not attached")
    # process any outstanding work before detaching
    self.tick()
    self.post_frame(Detach(local=self.local, remote=self.remote))
    self.detach_sent = True
    self.handle = None

  def close(self):
    self.local = None
    self.detach()

  def do_detach(self, detach):
    self.detach_rcvd = True
    self.remote = detach.local

  def do_disposition(self, delivery_tag, state, settled):
    if delivery_tag in self.unsettled:
      local, remote = self.unsettled[delivery_tag]
      remote.state = state
      remote.settled = settled
      remote.modified = True

  def do_flow(self, flow):
    self.do_flow_state(flow.flow_state)
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

    # we don't send flow state until the transfer_count is
    # initialized, this ensures an unambiguous calculation of the
    # initial transfer_count
    if self.echo and self.transfer_count is not None:
      self.post_frame(Flow(flow_state=self.flow_state()))

    role = self.session.roles[self.role]
    states = {}

    for dtag, local, remote in self.get_local(modified=True):
      if remote.settled:
        continue
      ids = role.transfers[(self, dtag)]
      if local in states:
        ranges = states[local]
      else:
        ranges = RangeSet()
        states[local] = ranges
      for r in ids:
        ranges.add_range(r)

      if local.settled:
        role.settle(self, dtag)
        self.unsettled.pop(dtag)
      local.modified = False

    for local, ranges in states.items():
      extents = []
      for r in ranges:
        ext = Extent(r.lower, r.upper, settled=local.settled, state=local.state)
        extents.append(ext)
      self.session.post_frame(Disposition(role=self.role, extents=extents))

class Sender(Link):

  # XXX
  role = False
  initial_count = 0

  def init(self):
    self.transfer_count = self.initial_count

  def do_flow_state(self, state):
    if state.transfer_count is None:
      receiver_count = self.initial_count
    else:
      receiver_count = state.transfer_count
    self.link_credit = receiver_count + state.link_credit - self.transfer_count
    self.drain = state.drain

  def drained(self):
    if self.drain:
      self.transfer_count += self.link_credit
      self.link_credit = 0
      self.post_frame(Flow(flow_state=self.flow_state()))

  def send(self, **kwargs):
    if self.link_credit <= 0:
      raise LinkError("would block")
    xfr = Transfer(**kwargs)
    if xfr.delivery_tag is None:
      xfr.delivery_tag = "%s" % self.delivery_count
    if not xfr.more:
      self.delivery_count += 1

    self.transfer_count += 1
    self.link_credit -= 1

    self.session.outgoing.append(self, xfr)
    if xfr.settled:
      self.session.outgoing.settle(self, xfr.delivery_tag)
    else:
      self.unsettled[xfr.delivery_tag] = (State(), State(xfr.state))

    xfr.flow_state = self.flow_state()
    self.post_frame(xfr)

    return xfr.delivery_tag

class Receiver(Link):

  # XXX
  role = True

  def init(self):
    self.incoming = []

  def do_transfer(self, xfr):
    self.session.incoming.append(self, xfr)
    self.incoming.append(xfr)
    self.unsettled[xfr.delivery_tag] = (State(), State(xfr.state, xfr.settled))
    self.do_flow_state(xfr.flow_state)

  def do_flow_state(self, state):
    if self.transfer_count is None:
      self.link_credit = state.link_credit
    else:
      self.link_credit -= state.transfer_count - self.transfer_count
    self.transfer_count = state.transfer_count
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
      return self.incoming.pop(0)
    else:
      raise LinkError("empty")

ROLES = {
  Sender.role: Sender,
  Receiver.role: Receiver
  }

def link(attach):
  cls = ROLES[not attach.role]
  return cls(attach.name, attach.remote, attach.local)
