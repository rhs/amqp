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

from codec import Value, Described, Primitive
from protocol import Header, DeliveryAnnotations, MessageAnnotations, \
    Properties, ApplicationProperties, Data, AmqpSequence, AmqpValue, Footer, \
    PROTOCOL_DECODER, PROTOCOL_ENCODER

CONVERSIONS = {
  "message_id": {long: Primitive("ulong"), int: Primitive("ulong")}
  }

class Message:

  def __init__(self, content=None, delivery_tag=None, **kwargs):
    self.delivery_tag = delivery_tag
    self.header = Header()
    self.annotations = None
    self.properties = Properties()
    self.application_properties = kwargs.pop("properties", None)
    self.content = content
    self.footer = Footer({})
    for k, v in kwargs.items():
      if k in CONVERSIONS:
        mappings = CONVERSIONS[k]
        t = type(v)
        if t in mappings:
          v = Value(mappings[t], v)
      for o in (self.header, self.properties, self.footer):
        if hasattr(o, k):
          setattr(o, k, v)

  def __getitem__(self, key):
    if self.application_properties:
      return self.application_properties[key]
    else:
      raise KeyError(key)

  def __setitem__(self, key, value):
    if not self.application_properties:
      self.application_properties = {}
    self.application_properties[key] = value

  def __repr__(self):
    args = []
    for f in ["delivery_tag", ("application_properties", "properties")] + \
          [f.name for f in Header.FIELDS] + \
          [f.name for f in Properties.FIELDS] + ["content"] + \
          [f.name for f in Footer.FIELDS]:
      if isinstance(f, tuple):
        f, a = f
      else:
        f, a = f, f
      for o in (self, self.header, self.properties, self.footer):
        if hasattr(o, f):
          v = getattr(o, f)
          if v is not None:
            args.append("%s=%r" % (a, v))
    return "Message(%s)" % ", ".join(args)

# XXX: encode: message -> str, decode: transfer -> message

def encode(message):
  encoder = PROTOCOL_ENCODER
  encoded = ""
  if message.header:
    encoded += encoder.encode(message.header)
  if message.properties:
    encoded += encoder.encode(message.properties)
  if message.application_properties:
    encoded += encoder.encode(ApplicationProperties(message.application_properties))
  if message.content is not None:
    # XXX: should dispatch
    if isinstance(message.content, str):
      encoded += encoder.encode(Data(message.content))
    elif isinstance(message.content, (list, tuple)):
      encoded += encoder.encode(AmqpSequence(message.content))
    else:
      encoded += encoder.encode(AmqpValue(message.content))
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
def process_application_properties(msg, ap):
  msg.application_properties = ap.value
def process_data(msg, v):
  if msg.content is None:
    msg.content = v.value
  else:
    msg.content += v.value
def process_sequence(msg, v):
  if msg.content is None:
    msg.content = list(v.value)
  else:
    msg.content.extend(v.value)
def process_value(msg, v):
  msg.content = v.value
def process_footer(msg, footer):
  msg.footer = footer

SECTION_PROCESSORS = {
  Header: process_header,
  DeliveryAnnotations: process_delivery_annotations,
  MessageAnnotations: process_message_annotations,
  Properties: process_properties,
  ApplicationProperties: process_application_properties,
  Data: process_data,
  AmqpSequence: process_sequence,
  AmqpValue: process_value,
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
