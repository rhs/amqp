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

from operations import Attach, Detach, Empty

class SessionError(Exception):
  pass

class Session:

  def __init__(self, name, factory):
    self.name = name
    self.factory = factory
    self.channel = None
    self.opening = True
    self.timeout = 0

    self.attaches_sent = 0
    self.attaches_rcvd = 0
    self.detaches_sent = 0
    self.detaches_rcvd = 0

    # XXX: this makes for some confusing terminology

    # sender state (from spec)
    self.outgoing = []
    self.command_id = 1
    self.acknowledged = 0
    self.command_limit = None
    # track commands we've 'acknowledged' but can't report yet
    self.ack_deferred = set()
    # command-id -> action to take when command is executed
    self.on_exe = {}

    # receiver state (from spec)
    self.received = None
    self.executed = None
    self.capacity = 1024
    self.syncpoint = None
    self.syncedto = None
    # track commands we've executed but can't report yet
    self.exe_deferred = set()
    # command-id -> action to take when command is 'acknowledged'
    self.on_ack = {}
    self.ack_actioned = 0

    # link name -> link end
    self.links = {}
    # handle -> link end
    self.handles = {}

    self.output = []

  def write(self, op):
    if op.executed is None:
      self.command_limit = self.command_id + op.capacity
    else:
      self.command_limit = op.executed + op.capacity
      self.do_exe(op.executed)
    self.do_ack(op.acknowledged)
    if op.COMMAND:
      self.received = op.command_id
    result = self.dispatch(op)
    if op.COMMAND and result:
      self.on_ack[op.command_id] = result
    if op.sync:
      if op.COMMAND:
        self.syncpoint = op.command_id
      else:
        # XXX: is this supposed to be correlated?
        self.flush()
    self.tick()

  def flush(self):
    self.write_op(Empty())

  def do_sync(self):
    if self.syncpoint is None or self.syncpoint > self.executed:
      return
    if self.syncedto < self.syncpoint:
      self.flush()
      self.syncpoint = None

  def do_exe(self, executed):
    idx = self.acknowledged
    while idx < executed:
      cmd = self.outgoing[idx - self.acknowledged]
      assert cmd.command_id <= executed
      if cmd.command_id in self.on_exe:
        pre = self.acknowledged
        self.on_exe.pop(cmd.command_id)(cmd)
        post = self.acknowledged
        idx += post - pre
      idx += 1

  def do_ack(self, acknowledged):
    while self.ack_actioned < acknowledged:
      next = self.ack_actioned + 1
      if next in self.on_ack:
        self.on_ack.pop(next)(next)
      self.ack_actioned = next

  def tick(self):
    pass

  def done(self, cmd):
    if cmd.command_id <= self.executed:
      return

    self.exe_deferred.add(cmd.command_id)
    while (self.executed + 1) in self.exe_deferred:
      self.executed += 1
      self.exe_deferred.discard(self.executed)

  def actioned(self, cmd):
    if cmd.command_id <= self.acknowledged:
      return

    self.ack_deferred.add(cmd.command_id)
    self.on_exe.pop(cmd.command_id, None)
    while (self.acknowledged + 1) in self.ack_deferred:
      self.acknowledged += 1
      self.outgoing.pop(0)
      self.ack_deferred.discard(self.acknowledged)

  # XXX: dup of read in framing
  def read(self, n=None):
    self.do_sync()
    result = self.output[:n]
    del self.output[:n]
    return result

  def dispatch(self, op):
    return getattr(self, "do_%s" % op.NAME, self.unhandled)(op)

  def unhandled(self, cmd):
    link = self.handles[cmd.handle]
    return link.write(cmd)

  def write_op(self, op):
    if not isinstance(op, Attach):
      assert self.attaches_sent > self.detaches_sent
    op.acknowledged = self.acknowledged
    op.executed = self.executed
    op.capacity = self.capacity
    op.command_id = self.command_id
    self.output.append(op)
    self.syncedto = self.executed

  def write_cmd(self, cmd, action=None):
    if action:
      cmd.sync = True
    self.outgoing.append(cmd)
    self.write_op(cmd)
    self.on_exe[cmd.command_id] = action or self.actioned
    self.command_id += 1

  def attaching(self):
    return self.attaches_rcvd > self.attaches_sent

  def detaching(self):
    return self.detaches_rcvd > self.detaches_sent

  def attach(self):
    if self.attaches_sent > self.detaches_sent:
      raise SessionError("double attach")
    self.write_op(Attach(name = self.name,
                         opening = self.opening,
                         timeout = self.timeout,
                         received = self.received,
                         acknowledged = self.acknowledged,
                         executed = self.executed,
                         capacity = self.capacity,
                         command_id = self.command_id))
    self.attaches_sent += 1

  def do_attach(self, att):
    if self.attaches_rcvd > self.detaches_rcvd:
      raise SessionError("double attach")
    else:
      self.attaches_rcvd += 1

      if att.opening:
        self.executed = att.command_id - 1

  def detach(self, closing, exception=None):
    if self.detaches_sent > self.attaches_sent:
      raise SessionError("double detach")
    # process any outstanding work before detaching
    self.tick()
    self.write_op(Detach(name = self.name,
                         closing = closing,
                         acknowledged = self.acknowledged,
                         executed = self.executed,
                         exception = exception))
    self.detaches_sent += 1

  def do_detach(self, det):
    if self.detaches_rcvd > self.attaches_rcvd:
      raise SessionError("double detach")
    else:
      self.detaches_rcvd += 1

  def do_empty(self, op):
    pass

  def do_link(self, link_cmd):
    if self.links.has_key(link_cmd.name):
      link = self.links[link_cmd.name]
    else:
      link = self.factory(link_cmd)
      self.add(link)

    self.handles[link_cmd.handle] = link
    self.done(link_cmd)
    link.write(link_cmd)

  def do_unlink(self, unlink):
    link = self.handles.pop(unlink.handle)
    self.done(unlink)
    link.write(unlink)

  def add(self, link):
    link.session = self
    link.handle = self.allocate_handle()
    self.links[link.name] = link

  def allocate_handle(self):
    return max([-1] + [l.handle for l in self.links.values()]) + 1

  def remove(self, link):
    link.session = None
    link.handle = None
    del self.links[link.name]
