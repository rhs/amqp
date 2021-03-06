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
from codec import Array, Described, Primitive, Value, Symbol
from util import load_xml, pythonize, decode_numeric_desc

class Field:

  def __init__(self, name, key, type, mandatory, multiple, default=None):
    self.name = name
    self.key = key
    self.type = type
    self.mandatory = mandatory
    self.multiple = multiple
    self.default = default

  def __repr__(self):
    return "Field(%r, %r, %r, %r, %r, %r)" % \
        (self.name, self.key, self.type, self.mandatory, self.multiple,
         self.default)

OPENS = "[("
CLOSES = ")]"

class Composite(object):

  @classmethod
  def construct(cls, t, v):
    return getattr(cls, "construct_%s" % cls.SOURCE.name)(t, v)

  @classmethod
  def construct_map(cls, t, m):
    return cls(**dict([(pythonize(k.name), v) for (k, v) in m.iteritems()]))

  @classmethod
  def construct_list(cls, t, l):
    return cls(*l)

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
    return getattr(self, "deconstruct_%s" % self.SOURCE.name)()

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
        value = Array(field.type, value)
      else:
        value = Value(field.type, value)
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

    if "frame" in self.ARCHETYPES or len(kwa) < len(ora):
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

class Restricted(object):

  @classmethod
  def construct(cls, t, v):
    return cls(v)

  def __init__(self, value):
    self.value = value

  def deconstruct(self):
    return self.value

  def __repr__(self):
    return "%s(%r)" % (self.__class__.__name__, self.value)

def resolve(name, aliases):
  while name in aliases:
    name = aliases[name]
  return name

# XXX: naming, this does restricted too
def load_composite(types, **kwargs):
  sources = {"*": None}
  descriptors = {}
  composite = []
  for nd in types:
    name = nd["@name"]
    if nd["@source"]:
      sources[name] = nd["@source"]
    if nd["descriptor"]:
      sym = Symbol(str(nd["descriptor/@name"]))
      num = Value(Primitive("ulong"),
                  decode_numeric_desc(nd["descriptor/@code"]))
      descriptors[name] = (num, sym)
      composite.append(nd)

  result = []

  for nd in composite:
    archetypes = \
        [p.strip() for p in pythonize(nd["@provides"] or "").split(",")]
    cls_name = pythonize(nd["@name"], camel=True)
    if cls_name in kwargs:
      bases = kwargs[cls_name]
    else:
      bases = ()
      for arch in archetypes:
        b = kwargs.get(arch, ())
        if inspect.isclass(b):
          bases += (b,)
        else:
          bases += b
      if not bases:
        bases = kwargs.get(nd["@class"], ())
    if inspect.isclass(bases):
      bases = (bases,)

    dict = {}
    dict["NAME"] = pythonize(nd["@name"])
    dict["ARCHETYPES"] = archetypes
    dict["DESCRIPTORS"] = descriptors.get(nd["@name"])
    dict["SOURCE"] = Primitive(pythonize(resolve(nd["@source"], sources)))
    dict["TYPE"] = Described(dict["DESCRIPTORS"][0], dict["SOURCE"])
    encoded = []
    for f in nd.query["field"]:
      d = descriptors.get(f["@type"])
      ftype = pythonize(f["@type"])
      if ftype == "*":
        ftype = None
      else:
        ftype = Primitive(pythonize(resolve(f["@type"], sources)))
      if d:
        ftype = Described(d[0], ftype)
      encoded.append(Field(pythonize(f["@name"]),
                           Symbol(f["@name"]),
                           ftype,
                           f["@mandatory"] == "true",
                           f["@multiple"] == "true"))
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
