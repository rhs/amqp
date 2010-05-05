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
from util import load_xml, pythonize, decode_numeric_desc

class Field:

  def __init__(self, name, default=None):
    self.name = name
    self.default = default

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

def load_compound(types, *default_bases, **kwargs):
  result = []
  for nd in types:
    archetype = pythonize(nd["@provides"])
    cls_name = pythonize(nd["@name"], camel=True)
    if cls_name in kwargs:
      bases = kwargs[cls_name]
    else:
      bases = kwargs.get(archetype, default_bases)
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
