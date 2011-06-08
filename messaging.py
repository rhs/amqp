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

from codec import Value
from protocol import Header, Properties, Footer, PROTOCOL_DECODER, PROTOCOL_ENCODER

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

# XXX: encode: message -> str, decode: transfer -> message

def encode(message):
  encoder = PROTOCOL_ENCODER
  encoded = ""
  if message.header:
    encoded += encoder.encode(message.header)
  if message.properties:
    encoded += encoder.encode(message.properties)
  if message.content is not None:
    # XXX: should dispatch
    if isinstance(message.content, str):
      encoded += encoder.encode(Value("binary", message.content,
                                      Value("long", 0x75)))
    elif isinstance(message.content, dict):
      encoded += encoder.encode(Value("map", message.content,
                                      Value("long", 0x77)))
    else:
      encoded += encoder.encode(Value("list", [message.content],
                                      Value("long", 0x76)))
  if message.footer:
    encoded += encoder.encode(message.footer)
  return encoded

def process_header(msg, header):
  msg.header = header
def process_properties(msg, props):
  msg.properties = props

def process_delivery_annotations(msg, ann):
  # XXX: we drop these
  print "warning, ignoring delivery annotations", ann
def process_message_annotations(msg, ann):
  msg.annotations = ann
def process_application_properties(msg, props):
  # XXX: we don't do anything with this yet
  print "warning, ignoring app properties", props
def process_data(msg, v):
  msg.content = v.value
def process_sequence(msg, v):
  if msg.content is None:
    msg.content = []
  else:
    msg.content.extend(v.value)
def process_mappings(msg, v):
  if msg.content is None:
    msg.content = {}
  else:
    msg.content.update(v.value)

VALUE_PROCESSORS = {
  0x71: process_delivery_annotations,
  0x72: process_message_annotations,
  0x74: process_application_properties,
  0x75: process_data,
  0x76: process_sequence,
  0x77: process_mappings
  }

def process_value(msg, value):
  VALUE_PROCESSORS[value.descriptor](msg, value)
def process_footer(msg, footer):
  msg.footer = footer

SECTION_PROCESSORS = {
  Header: process_header,
  Properties: process_properties,
  Value: process_value,
  Footer: process_footer
  }

def decode(transfer):
  message = Message()
  message.delivery_tag = transfer.delivery_tag
  remaining = transfer.payload
  sections = []
  while remaining:
    sect, remaining = PROTOCOL_DECODER.decode(remaining)
    sections.append(sect)
  while sections:
    sect = sections.pop(0)
    SECTION_PROCESSORS[sect.__class__](message, sect)
  return message
