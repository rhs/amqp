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

from protocol import Transfer
from util import Constant
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

ATTACHED = Constant("ATTACHED")
DETACHED = Constant("DETACHED")

class Link(object):

  def __init__(self, name, local, remote=None):
    self.name = name
    self.local = local
    self.remote = remote

    self.session = None
    self.handle = None

    # local and remote state can be None, ATTACHED, DETACHED
    self.local_state = None
    self.remote_state = None

    # flow state
    self.transfer_count = None
    self.link_credit = 0
    self.available = 0
    self.drain = False
    self.modified = False

    # used to provide default delivery-tag
    self.delivery_count = 0

    # delivery-tag -> (local_state, remote_state)
    self.unsettled = {}

    self.init()

  # XXX: should update these to use local and remote and/or add
  # versions for attaching/detaching
  def opening(self):
    return self.local_state is None and self.remote_state is ATTACHED

  def closing(self):
    return self.remote_state is DETACHED and self.local_state is ATTACHED

  def opened(self):
    return self.local_state is ATTACHED and self.remote_state is ATTACHED

  def closed(self):
    return self.local_state is DETACHED and self.remote_state is DETACHED

  def write(self, cmd):
    self.dispatch(cmd)

  def dispatch(self, cmd):
    return getattr(self, "do_%s" % cmd.NAME)(cmd)

  def attach(self):
    if self.local_state is ATTACHED:
      raise LinkError("already attached")
    self.local_state = ATTACHED
    self.modified = True

  def do_attach(self, attach):
    self.remote_state = ATTACHED
    self.remote = attach.local
    self.do_flow(attach.flow_state)

  # XXX: closing and errors
  def detach(self):
    if self.local_state is not ATTACHED:
      raise LinkError("not attached")
    self.local_state = DETACHED

  def close(self):
    self.local = None
    self.detach()

  def do_detach(self, detach):
    self.remote_state = DETACHED
    self.remote = detach.local

  def do_disposition(self, delivery_tag, state, settled):
    if delivery_tag in self.unsettled:
      local, remote = self.unsettled[delivery_tag]
      remote.state = state
      remote.settled = settled
      remote.modified = True

  def _query(self, index, settled=None, modified=None):
    return [(delivery_tag, pair[index])
            for delivery_tag, pair in self.unsettled.items()
            if (settled is None or settled == pair[index].settled) and
            (modified is None or modified == pair[index].modified)]

  def get_local(self, settled=None, modified=None):
    return self._query(0, settled, modified)

  def get_remote(self, settled=None, modified=None):
    return self._query(1, settled, modified)

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


class Sender(Link):

  # XXX
  role = False
  initial_count = 0

  def init(self):
    self.transfer_count = self.initial_count
    self.outgoing = []

  def do_flow(self, state):
    if state.transfer_count is None:
      receiver_count = self.initial_count
    else:
      receiver_count = state.transfer_count
    self.link_credit = receiver_count + state.link_credit - self.transfer_count
    self.drain = state.drain

  def capacity(self):
    return self.link_credit - len(self.outgoing)

  def drained(self):
    if self.drain:
      self.transfer_count += self.capacity()
      self.link_credit = len(self.outgoing)
      self.modified = True

  def send(self, **kwargs):
    if self.capacity() <= 0:
      raise LinkError("would block")
    xfr = Transfer(**kwargs)
    if xfr.delivery_tag is None:
      xfr.delivery_tag = "%s" % self.delivery_count
    if not xfr.more:
      self.delivery_count += 1
    self.outgoing.append(xfr)
    self.unsettled[xfr.delivery_tag] = (State(), State(xfr.state, xfr.settled))
    return xfr.delivery_tag

  def pending(self):
    return len(self.outgoing)

  def pop(self):
    xfr = self.outgoing.pop(0)
    self.transfer_count += 1
    self.link_credit -= 1
    return xfr

class Receiver(Link):

  # XXX
  role = True

  def init(self):
    self.incoming = []

  def do_transfer(self, xfr):
    self.incoming.append(xfr)
    self.unsettled[xfr.delivery_tag] = (State(), State(xfr.state, xfr.settled))
    # XXX: should we do this from session?
    self.do_flow(xfr.flow_state)

  def do_flow(self, state):
    if self.transfer_count is None:
      self.link_credit = state.link_credit
    else:
      self.link_credit -= state.transfer_count - self.transfer_count
    self.transfer_count = state.transfer_count
    self.available = state.available

  def capacity(self):
    return self.link_credit

  def flow(self, n, drain=False):
    self.link_credit += n
    self.drain = drain
    self.modified = True

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
