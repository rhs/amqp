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

import inspect

from framing import ConnectionFrame, SessionFrame
from util import load_xml, pythonize, decode_numeric_desc

class Field:

  def __init__(self, name):
    self.name = name
    self.default = None

  def __repr__(self):
    return "Field(%r, %r)" % (self.name, self.default)

class Compound(object):

  def __init__(self, *args, **kwargs):
    args = list(args)
    for f in self.FIELDS:
      if args:
        v = args.pop(0)
      else:
        v = kwargs.pop(f.name, f.default)
      setattr(self, f.name, v)

    if args:
      raise TypeError("%s takes at most %s arguments (%s given))" %
                      (self.__class__.__name__, len(self.FIELDS),
                       len(self.FIELDS) + len(args)))

    if kwargs:
      raise TypeError("got unexpected keyword argument '%s'" % kwargs.keys()[0])

  def deconstruct(self):
    return [getattr(self, f.name) for f in self.ENCODED_FIELDS]

  def _args(self):
    args = []
    for f in self.FIELDS:
      v = getattr(self, f.name)
      if v != f.default:
        args.append("%s=%r" % (f.name, v))
    return args

  def __repr__(self):
    return "%s(%s)" % (self.__class__.__name__, ", ".join(self._args()))

class Operation(Compound):
  FIELDS = [Field("channel")]

class ConnectionOp(Operation):

  def frame(self, enc):
    return ConnectionFrame(0, self.channel, enc.encode(self))

class SessionOp(Operation):
  FIELDS = [Field("acknowledged"),
            Field("executed"),
            Field("capacity"),
            Field("command_id"),
            Field("sync")]

  COMMAND = False

  def frame(self, enc):
    flags = 0
    # XXX: hardcoded flags
    if self.COMMAND:
      flags |= 0x1
    if self.executed is None:
      flags |= 0x2
    if self.sync:
      flags |= 0x4
    # XXX: default these all to zero until I build the machinery to
    # set/use them properly
    return SessionFrame(flags, self.channel,
                        self.acknowledged or 0,
                        self.executed or 0,
                        self.capacity or 0,
                        self.command_id or 0,
                        enc.encode(self))

class Command(SessionOp):
  COMMAND = True

def load_compound(sections, *default_bases, **kwargs):
  result = []
  for nd in sections.query["type", lambda n: n["@class"] == "compound"]:
    cls_name = pythonize(nd["@name"], camel=True)
    bases = kwargs.get(cls_name, default_bases)
    if inspect.isclass(bases):
      bases = (bases,)

    dict = {}
    dict["NAME"] = pythonize(nd["@name"])
    dict["DESCRIPTORS"] = (str(nd["descriptor/@name"]),
                           decode_numeric_desc(nd["descriptor/@code"]))
    encoded = [Field(pythonize(f["@name"])) for f in nd.query["field"]]
    dict["ENCODED_FIELDS"] = encoded
    fields = encoded + \
        [f
         for b in bases
         for c in inspect.getmro(b) if c.__dict__.has_key("FIELDS")
         for f in c.FIELDS ]
    fieldnames = set([f.name for f in fields])
    assert len(fieldnames) == len(fields), "duplicate fields"
    dict["FIELDS"] = fields
    result.append(type(cls_name, bases, dict))
  return result

TRANSPORT = load_xml("transport.xml")
def named(name):
  return lambda nd: nd["@name"] == name
COMPOUND = \
    load_compound(TRANSPORT["amqp/section", named("controls")], Operation,
                  Open=ConnectionOp,
                  Attach=SessionOp,
                  Detach=SessionOp,
                  Close=ConnectionOp) + \
    load_compound(TRANSPORT["amqp/section", named("commands")], Command) + \
    load_compound(TRANSPORT["amqp/section", named("definitions")], Compound)

__all__ = ["COMPOUND"]

for cls in COMPOUND:
  globals()[cls.__name__] = cls
  __all__.append(cls.__name__)
