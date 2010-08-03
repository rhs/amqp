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
from codec import Box, Described, Symbol
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

OPENS = "[("
CLOSES = ")]"

class Composite(object):

  def __init__(self, *args, **kwargs):
    args = list(args)
    for f in self.FIELDS:
      if args:
        v = args.pop(0)
        if f.multiple:
          if isinstance(v, Described) and v.descriptor == True:
            v = v.value
          else:
            v = [v]
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
    elif field.type is not None and field.category != "composite":
      if field.multiple:
        value = [Box(field.type, v) for v in value]
      else:
        value = Box(field.type, value)
    if field.multiple:
      value = Described(True, value)
    return value

  def _defaulted(self, field):
    return field.default == getattr(self, field.name)

  def _format(self, field, multiline=True):
    value = getattr(self, field.name)
    if multiline and field.multiple and value is not None and len(value) > 1:
      return ("[\n%s\n]" % ",\n".join(map(repr, value))).replace("\n", "\n  ")
    else:
      return repr(value)

  def _ordinal_args(self, multiline=False):
    fields = []
    for f in self.FIELDS:
      v = getattr(self, f.name)
      fields.append(f)
    while fields and self._defaulted(fields[-1]):
      fields.pop()
    return map(lambda f: self._format(f, multiline), fields)

  def _keyword_args(self, multiline=False):
    args = []
    for f in self.FIELDS:
      if not self._defaulted(f):
        args.append("%s=%s" % (f.name, self._format(f, multiline)))
    return args

  def _wrap(self, st, length, first=None):
    lines = st.split("\n")
    result = ""
    count = 0
    limit = first or length
    for i in range(len(lines)):
      line = lines[i]
      final = i == len(lines) - 1

      last = result[-1:]
      if last in OPENS:
        stripped = line.lstrip()
      else:
        stripped = " %s" % line.lstrip()

      if len(stripped) > 1 and stripped[0] == " " and stripped[1] in CLOSES:
        stripped = stripped[1:]

      if count + len(stripped) <= limit or final and stripped in CLOSES:
        result += stripped
        count += len(stripped)
      else:
        result += "\n" + line
        count = len(line)
        limit = length

    return result.strip()

  def format(self, multiline=False):
    if multiline:
      sep = ",\n"
    else:
      sep = ", "

    kwa = sep.join(self._keyword_args(multiline))
    ora = sep.join(self._ordinal_args(multiline))

    if self.ARCHETYPE == "frame" or len(kwa) < len(ora):
      a = kwa
    else:
      a = ora

    name = self.__class__.__name__
    if multiline:
      fmt = "%s(\n%s\n)"
    else:
      fmt = "%s(%s)"
    result = (fmt % (name, a)).replace("\n", "\n  ")
    if multiline:
      return self._wrap(result, 64, 61 - len(name))
    else:
      return result

  def __repr__(self):
    return self.format()

def resolve(name, aliases):
  while name in aliases:
    name = aliases[name]
  return name

def load_composite(types, *default_bases, **kwargs):
  aliases = {"*": None}
  classes = {}
  composite = []
  for nd in types:
    name = nd["@name"]
    cls = nd["@class"]
    classes[name] = cls
    if cls == "restricted":
      aliases[name] = nd["@source"]
    elif cls == "composite":
      composite.append(nd)

  result = []

  for nd in composite:
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
    dict["ARCHETYPE"] = archetype
    dict["DESCRIPTORS"] = (Symbol(str(nd["descriptor/@name"])),
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
