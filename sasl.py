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

from dispatcher import Dispatcher
from framing import SASL_FRAME
from protocol import *


class SASL(Dispatcher):

  type_decoder = PROTOCOL_DECODER
  type_encoder = PROTOCOL_ENCODER

  def __init__(self, connection):
    Dispatcher.__init__(self, 3, SASL_FRAME)
    self.connection = connection
    self.mechanisms = None
    self.mechanism = None
    self.username = None
    self.password = None
    self.output_redirect = False
    self.outcome = None

  def client(self, mechanism="ANONYMOUS", username=None, password=None):
    self.mechanism = mechanism
    self.username = username
    self.password = password
    self.post_frame(0, SaslInit(mechanism = self.mechanism,
                                initial_response = "\0%s\0%s" % (self.username or "",
                                                                 self.password or "")))

  def server(self, mechanisms=("ANONYMOUS", "PLAIN"), passwords={}):
    self.mechanisms = mechanisms
    self.passwords = passwords
    self.post_frame(0, SaslMechanisms(sasl_server_mechanisms = self.mechanisms))

  def do_sasl_mechanisms(self, channel, mechs):
    if self.mechanism not in [m.name for m in mechs.sasl_server_mechanisms]:
      # we're pretending negotiation failure is an outcome
      self.outcome = 2

  def do_sasl_init(self, channel, init):
    mech = init.mechanism.name
    if mech in self.mechanisms:
      return getattr(self, "mech_%s" % mech.lower())(init.initial_response)
    else:
      return self.post_outcome(2)

  def mech_anonymous(self, resp):
    return self.post_outcome(0)

  def mech_plain(self, resp):
    _, username, password = resp.split("\x00")
    if username in self.passwords and password == self.passwords[username]:
      return self.post_outcome(0)
    else:
      return self.post_outcome(1)

  def post_outcome(self, code):
    self.outcome = code
    self.post_frame(0, SaslOutcome(code=code))
    if code == 0:
      self.output_redirect = True
      return self.__tunnel
    else:
      # XXX: how do we close things down?
      pass

  def do_sasl_outcome(self, channel, outcome):
    self.outcome = outcome.code
    if self.outcome == 0:
      self.output_redirect = True
      return self.__tunnel

  def unhandled(self, channel, body):
    raise ValueError("unknown boyd: %s" % body);

  def tick(self):
    if self.output_redirect:
      self.output.write(self.connection.read())

  def __tunnel(self):
    self.connection.write(self.input.read())
