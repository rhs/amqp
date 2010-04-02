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

from operations import Flow, Link as LinkCmd, Unlink, Transfer, Disposition
from uuid import uuid4

class LinkError(Exception):
  pass

PENDING = object()

class Link(object):

  def __init__(self, name, source, target):
    self.name = name
    self.source = source
    self.target = target

    self.session = None
    self.handle = None

    self.links_rcvd = 0
    self.links_sent = 0
    self.unlinks_rcvd = 0
    self.unlinks_sent = 0

    self.limit = 0
    self.count = 0
    self.link_tag = uuid4()

    # delivery-tag -> disposition
    self.unsettled = {}

    self.init()

  def write_cmd(self, cmd, action=None):
    cmd.handle = self.handle
    self.session.write_cmd(cmd, action)

  def done(self, cmd):
    self.session.done(cmd)

  def actioned(self, cmd):
    self.session.actioned(cmd)

  def write(self, cmd):
    return self.dispatch(cmd)

  def dispatch(self, cmd):
    return getattr(self, "do_%s" % cmd.NAME)(cmd)

  def opening(self):
    return self.links_sent < self.links_rcvd

  def closing(self):
    return self.unlinks_sent < self.unlinks_rcvd

  def opened(self):
    return self.links_rcvd == self.links_sent and self.links_sent > self.unlinks_sent

  def closed(self):
    return self.unlinks_rcvd == self.unlinks_sent and self.unlinks_sent == self.links_sent

  def capacity(self):
    return self.limit - self.count

  def link(self):
    self.write_cmd(LinkCmd(name = self.name,
                           direction = self.direction,
                           source = self.source,
                           target = self.target))
    self.links_sent += 1

  def do_link(self, link_cmd):
    if self.links_rcvd > self.unlinks_rcvd:
      raise LinkError("double link")
    else:
      if self.direction == Sender.direction:
        self.target = link_cmd.target
      else:
        self.source = link_cmd.source
      self.links_rcvd += 1

  def unlink(self):
    self.write_cmd(Unlink())
    self.unlinks_sent += 1
    self.handle = None

  def do_unlink(self, unlink):
    if self.unlinks_rcvd > self.links_rcvd:
      raise LinkError("double unlink")
    else:
      self.unlinks_rcvd += 1

class Sender(Link):

  # XXX
  direction = 1

  def init(self):
    self.outgoing = []

  def get_local(self):
    return self.source

  def set_local(self, value):
    self.source = value

  local = property(fget=get_local, fset=set_local)

  def get_remote(self):
    return self.target

  def set_remote(self, value):
    self.target = value

  remote = property(fget=get_remote, fset=set_remote)

  def do_flow(self, flow):
    self.limit = flow.limit
    self.done(flow)

  def send(self, **kwargs):
    if self.count >= self.limit:
      raise LinkError("would block")
    xfr = Transfer(**kwargs)
    if xfr.delivery_tag is None:
      xfr.delivery_tag = "%s:%s" % (self.link_tag, self.count)
    self.count += 1
    self.write_cmd(xfr, self.transferred)
    self.outgoing.append(xfr)
    self.unsettled[xfr.delivery_tag] = PENDING
    return xfr.delivery_tag

  def transferred(self, xfr):
    if self.unsettled.get(xfr.delivery_tag) is PENDING:
      self.unsettled[xfr.delivery_tag] = None
    if xfr.delivery_tag not in self.unsettled:
      self.actioned(xfr)

  def do_disposition(self, disp):
    on = False
    for xfr in self.outgoing:
      if xfr.delivery_tag == disp.first:
        on = True
      if on:
        self.unsettled[xfr.delivery_tag] = disp.disposition
      if xfr.delivery_tag == disp.last:
        break
    self.done(disp)

  def pending(self):
    return len([d for d in self.unsettled.values() if d is not PENDING])

  def get(self):
    if self.outgoing:
      xfr = self.outgoing[0]
      d = self.unsettled[xfr.delivery_tag]
      return xfr.delivery_tag, d
    else:
      raise LinkError("empty")

  def settle(self, delivery_tag):
    if delivery_tag not in self.unsettled:
      return
    idx = 0
    while idx < len(self.outgoing):
      xfr = self.outgoing[idx]
      if xfr.delivery_tag == delivery_tag:
        del self.outgoing[idx]
        self.unsettled.pop(xfr.delivery_tag)
        self.actioned(xfr)
        break
      else:
        idx += 1

class Receiver(Link):

  # XXX
  direction = 0

  def init(self):
    self.incoming = []

  def get_local(self):
    return self.target

  def set_local(self, value):
    self.target = value

  local = property(fget=get_local, fset=set_local)

  def get_remote(self):
    return self.source

  def set_remote(self, value):
    self.source = value

  remote = property(fget=get_remote, fset=set_remote)

  def do_transfer(self, xfr):
    self.count += 1
    self.incoming.append(xfr)
    self.unsettled[xfr.delivery_tag] = PENDING
    return lambda id: self.unsettled.pop(xfr.delivery_tag)

  def flow(self, n):
    self.limit += n
    self.write_cmd(Flow(limit=self.limit))

  def pending(self):
    return len(self.incoming)

  def get(self):
    if self.incoming:
      return self.incoming.pop(0)
    else:
      raise LinkError("empty")

  def ack(self, xfr, **disposition):
    self.done(xfr)
    if disposition:
      self.unsettled[xfr.delivery_tag] = disposition
      self.write_cmd(Disposition(disposition=disposition,
                                 first=xfr.delivery_tag,
                                 last=xfr.delivery_tag))
    else:
      self.unsettled[xfr.delivery_tag] = None

DIRECTIONS = {
  Sender.direction: Sender,
  Receiver.direction: Receiver
  }

def link(link_cmd):
  cls = DIRECTIONS[1 - link_cmd.direction]
  return cls(link_cmd.name, link_cmd.source, link_cmd.target)
