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
from codec import Box
from util import load_xml, pythonize, decode_numeric_desc

class Field:

  def __init__(self, name, type, mandatory, multiple, category, default=None):
    self.name = name
    self.type = type
    self.mandatory = mandatory
    self.multiple = multiple
    self.category = category
    self.default = default

  def __repr__(self):
    return "Field(%r, %r, %r, %r, %r, %r)" % \
        (self.name, self.type, self.mandatory, self.multiple, self.category,
         self.default)

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
    return [self.deconstruct_field(f) for f in self.ENCODED_FIELDS]

  def deconstruct_field(self, field):
    value = getattr(self, field.name)
    if value is None:
      if field.mandatory:
        raise ValueError("%s: field %s is mandatory" % (self, field.name))
    elif field.type is not None and field.category != "compound":
      value = Box(field.type, value)
    return value

  def _args(self):
    args = []
    for f in self.FIELDS:
      v = getattr(self, f.name)
      if v != f.default:
        args.append("%s=%r" % (f.name, v))
    return args

  def __repr__(self):
    return "%s(%s)" % (self.__class__.__name__, ", ".join(self._args()))

def resolve(name, aliases):
  while name in aliases:
    name = aliases[name]
  return name

def load_compound(types, *default_bases, **kwargs):
  aliases = {"*": None}
  classes = {}
  compound = []
  for nd in types:
    name = nd["@name"]
    cls = nd["@class"]
    classes[name] = cls
    if cls == "restricted":
      aliases[name] = nd["@source"]
    elif cls == "compound":
      compound.append(nd)

  result = []

  for nd in compound:
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
    encoded = [Field(pythonize(f["@name"]),
                     pythonize(resolve(f["@type"], aliases)),
                     f["@mandatory"] == "true",
                     f["@multiple"] == "true",
                     pythonize(classes.get(f["@type"])))
               for f in nd.query["field"]]
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
