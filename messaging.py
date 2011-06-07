#!/usr/bin/python
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

from protocol import Fragment, Header, Properties, Footer
from connection import Connection

class Message:

  def __init__(self, content=None, delivery_tag=None, **kwargs):
    self.delivery_tag = delivery_tag
    self.header = Header()
    self.annotations = None
    self.properties = Properties()
    self.content = content
    self.footer = Footer()
    for k, v in kwargs.items():
      for o in (self.header, self.properties, self.footer):
        if hasattr(o, k):
          setattr(o, k, v)

  def __repr__(self):
    args = []
    for f in ["delivery_tag"] + [f.name for f in Header.FIELDS] + \
          [f.name for f in Properties.FIELDS] + ["content"] + \
          [f.name for f in Footer.FIELDS]:
      for o in (self, self.header, self.properties, self.footer):
        if hasattr(o, f):
          v = getattr(o, f)
          if v is not None:
            args.append("%s=%r" % (f, v))
    return "Message(%s)" % ", ".join(args)

# XXX: encode(message) -> fragments, decode(transfer) -> message

def encode(message):
  encoder = Connection.type_encoder
  # XXX: constants
  head = Fragment(True, True, 0, 0, 0, encoder.encode(message.header))
  prop = Fragment(True, True, 3, 1, 0, encoder.encode(message.properties))
  body = Fragment(True, True, 5, 2, 0, message.content)
  foot = Fragment(True, True, 9, 3, 0, encoder.encode(message.footer))
  return (head, prop, body, foot)

def process_header(msg, bytes):
  msg.header = Connection.type_decoder.decode(bytes)[0]
def process_delivery_annotations(msg, bytes):
  # XXX: we drop these
  print "warning, ignoring delivery annotations"
def process_message_annotations(msg, bytes):
  msg.annotations = Connection.type_decoder.decode(bytes)[0]
def process_properties(msg, bytes):
  msg.properties = Connection.type_decoder.decode(bytes)[0]
def process_application_properties(msg, bytes):
  # XXX: we don't do anything with this yet
  print "warning, ignoring app properties"
def process_data(msg, bytes):
  msg.content = bytes
def process_amqp_data(msg, bytes):
  msg.content = Connection.type_decoder.decode(bytes)[0]
def process_footer(msg, bytes):
  msg.footer = Connection.type_decoder.decode(bytes)[0]

SECTION_PROCESSORS = {
  0: process_header,
  1: process_delivery_annotations,
  2: process_message_annotations,
  3: process_properties,
  4: process_application_properties,
  5: process_data,
  6: process_amqp_data,
  7: process_amqp_data,
  8: process_amqp_data,
  9: process_footer
  }

def decode(transfer):
  message = Message()
  message.delivery_tag = transfer.delivery_tag
  fragments = transfer.fragments
  sections = []
  for f in fragments:
    if f.first:
      sections.append([f.section_code, ""])
    sections[-1][-1] += f.payload
  while sections:
    code, payload = sections.pop(0)
    SECTION_PROCESSORS[code](message, payload)
  return message
