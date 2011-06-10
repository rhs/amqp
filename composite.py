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
from codec import Array, Value, Symbol, UNDESCRIBED
from util import load_xml, pythonize, decode_numeric_desc

class Field:

  def __init__(self, name, key, type, source, descriptor, mandatory, multiple,
               category, default=None):
    self.name = name
    self.key = key
    self.type = type
    self.source = source
    self.descriptor = descriptor
    self.mandatory = mandatory
    self.multiple = multiple
    self.category = category
    self.default = default

  def __repr__(self):
    return "Field(%r, %r, %r, %r, %r, %r, %r, %r, %r)" % \
        (self.name, self.key, self.type, self.source, self.descriptor,
         self.mandatory, self.multiple, self.category, self.default)

OPENS = "[("
CLOSES = ")]"

class Composite(object):

  def __init__(self, *args, **kwargs):
    args = list(args)
    for f in self.FIELDS:
      if args:
        v = args.pop(0)
        if f.multiple:
          if isinstance(v, Array):
            v = v.values
          elif v is not None:
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
    return getattr(self, "deconstruct_%s" % self.SOURCE)()

  def deconstruct_list(self):
    return [self.deconstruct_field(f) for f in self.ENCODED_FIELDS]

  def deconstruct_map(self):
    result = {}
    for f in self.ENCODED_FIELDS:
      if not self._defaulted(f):
        result[f.key] = self.deconstruct_field(f)
    return result

  def deconstruct_field(self, field):
    value = getattr(self, field.name)
    if value is None:
      if field.mandatory:
        raise ValueError("%s: field %s is mandatory" % (self, field.name))
    elif field.type is not None:
      if field.multiple:
        value = Array(field.source, value, field.descriptor)
      else:
        value = Value(field.source, value, field.descriptor)
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
  sources = {"*": None}
  classes = {}
  composite = []
  for nd in types:
    name = nd["@name"]
    cls = nd["@class"]
    if nd["descriptor"]:
      sym = Symbol(str(nd["descriptor/@name"]))
      num = Value("ulong", decode_numeric_desc(nd["descriptor/@code"]))
      classes[name] = (cls, sym, num)
    else:
      classes[name] = (cls, UNDESCRIBED, UNDESCRIBED)
    if nd["@source"]:
      sources[name] = nd["@source"]
    if cls == "composite":
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
    dict["DESCRIPTORS"] = classes.get(nd["@name"])[1:]
    dict["SOURCE"] = pythonize(nd["@source"])
    encoded = []
    for f in nd.query["field"]:
      category, sym, num = classes.get(f["@type"], (None, UNDESCRIBED, UNDESCRIBED))
      ftype = pythonize(f["@type"])
      if ftype == "*":
        ftype = None
      encoded.append(Field(pythonize(f["@name"]),
                           Symbol(f["@name"]),
                           ftype,
                           pythonize(resolve(f["@type"], sources)),
                           num,
                           f["@mandatory"] == "true",
                           f["@multiple"] == "true",
                           pythonize(category)))
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
