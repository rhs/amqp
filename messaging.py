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

class Message:

  def __init__(self, content=None, delivery_tag=None, **kwargs):
    self.delivery_tag = delivery_tag
    self.header = Header()
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
# XXX: frag + defrag

def encode(message, encoder):
  # XXX: constants
  head = Fragment(True, True, 0, 0, encoder.encode(message.header))
  prop = Fragment(True, True, 1, len(head.payload),
                  encoder.encode(message.properties))
  body = Fragment(True, True, 3, prop.fragment_offset + len(prop.payload),
                  message.content)
  foot = Fragment(True, True, 2, body.fragment_offset + len(body.payload),
                  encoder.encode(message.footer))
  return (head, prop, body, foot)

def decode(transfer, decoder):
  message = Message()
  message.delivery_tag = transfer.delivery_tag
  fragments = transfer.fragments
  message.header = decoder.decode(fragments[0].payload)[0]
  message.properties = decoder.decode(fragments[1].payload)[0]
  message.content = fragments[2].payload
  # XXX: constants
  if fragments[-1].format_code == 2:
    message.footer = decoder.decode(fragments[-1].payload)[0]
  return message
