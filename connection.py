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

from protocol import *

from dispatcher import Dispatcher
from codec import TypeDecoder, TypeEncoder
from util import pythonize
from uuid import uuid4


class ConnectionError(Exception):
  pass

class Connection(Dispatcher):

  # XXX: these should go someplace more central
  type_decoder = TypeDecoder()
  type_encoder = TypeEncoder()

  for cls in CLASSES:
    # XXX: should index some of this stuff and move it elsewhere
    type_encoder.deconstructors[cls] = lambda v: (v.DESCRIPTORS[0],
                                                  v.deconstruct())
    for d in cls.DESCRIPTORS:
      if cls.SOURCE == "map":
        const = lambda d, m, c=cls: c(**dict([(pythonize(k.name), v)
                                              for (k, v) in m.iteritems()]))
      else:
        const = lambda d, l, c=cls: c(*l)
      type_decoder.constructors[d] = const

  def __init__(self, factory):
    Dispatcher.__init__(self)
    self.factory = factory

    self.open_rcvd = False
    self.open_sent = False
    self.close_rcvd = False
    self.close_sent = False

    # incoming channel -> session
    self.incoming = {}
    # outgoing channel -> session
    self.outgoing = {}

  def opening(self):
    return self.open_rcvd and not self.open_sent

  def opened(self):
    return self.open_rcvd and self.open_sent and \
        not (self.close_rcvd or self.close_sent)

  def closed(self):
    self.close_rcvd and self.close_sent

  def closing(self):
    return self.close_rcvd and not self.close_sent

  def unhandled(self, channel, body):
    ssn = self.incoming[channel]
    ssn.write(body)

  def tick(self):
    for ch, ssn in self.outgoing.items():
      ssn.tick()
      for body in ssn.read():
        self.post_frame(ssn.channel, body)

  def open(self, *args, **kwargs):
    self.post_frame(0, Open(*args, **kwargs))
    self.open_sent = True

  def do_open(self, channel, open):
    if self.open_rcvd:
      self.close(ConnectionError(error_code=501, description="double open"))
    else:
      self.open_rcvd = True

  def close(self, *args, **kwargs):
    # avoid stranding frames inside sessions
    self.tick()
    self.post_frame(0, Close(*args, **kwargs))
    self.close_sent = True

  def do_close(self, channel, close):
    if not self.close_rcvd:
      self.close_rcvd = True

  def add(self, ssn):
    ssn.channel = self.allocate_channel()
    self.outgoing[ssn.channel] = ssn

  def allocate_channel(self):
    return max([-1] + self.outgoing.keys()) + 1

  def remove(self, ssn):
    # avoid stranding frames inside sessions
    self.tick()
    if ssn.channel in self.outgoing and self.outgoing[ssn.channel] == ssn:
      del self.outgoing[ssn.channel]
      ssn.channel = None
    else:
      raise ConnectionError("no such session")

  def do_begin(self, channel, begin):
    if channel in self.incoming:
      raise ConnectionError("double begin")

    if begin.remote_channel in self.outgoing:
      ssn = self.outgoing[begin.remote_channel]
    else:
      ssn = self.factory(begin.properties)
      ssn.remote_channel = channel
      self.add(ssn)

    self.incoming[channel] = ssn
    ssn.write(begin)

  def do_end(self, channel, end):
    if channel not in self.incoming:
      raise ConnectionError("double end")

    ssn = self.incoming.pop(channel)
    ssn.write(end)
