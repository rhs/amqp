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

import datetime, struct, time, uuid, cStringIO
from util import pythonize, load_xml, identity, Constant

class Type:
  pass

class Described(Type):

  def __init__(self, descriptor, source):
    self.descriptor = descriptor
    self.source = source

  def construct(self, factory, value):
    return factory.construct_described(self, value)

  def encode(self, encoder, value):
    return encoder.encode_described(self, value)

  def __hash__(self):
    return hash((self.descriptor, self.source))

  def __cmp__(self, o):
    if isinstance(o, Described):
      return cmp((self.descriptor, self.source), (o.descriptor, o.source))
    else:
      return -1

  def __repr__(self):
    return "Described(%r, %r)" % (self.descriptor, self.source)

class Primitive(Type):

  def __init__(self, name):
    self.name = name

  def construct(self, factory, value):
    return factory.construct_primitive(self, value)

  def encode(self, encoder, value):
    return encoder.encode_primitive(self, value)

  def __hash__(self):
    return hash(self.name)

  def __cmp__(self, o):
    if isinstance(o, Primitive):
      return cmp(self.name, o.name)
    else:
      return -1

  def __repr__(self):
    return "Primitive(%r)" % self.name

class Array:

  def __init__(self, type, values):
    self.type = type
    self.values = values

  def __repr__(self):
    return "Array(%r, %r)" % (self.type, self.values)

class Value:

  def __init__(self, type, value):
    self.type = type
    self.value = value

  def __repr__(self):
    return "Value(%r, %r)" % (self.type, self.value)

  def __hash__(self):
    return hash(self.value)

  def __cmp__(self, o):
    if isinstance(o, Value):
      o = o.value
    return cmp(self.value, o)

class Symbol:

  def __init__(self, name):
    self.name = name

  def __hash__(self):
    return hash(self.name)

  def __eq__(self, o):
    return isinstance(o, Symbol) and self.name == o.name

  def __repr__(self):
    return "Symbol(%r)" % self.name

class Binary:

  def __init__(self, bytes):
    self.bytes = bytes

  def __hash__(self):
    return hash(self.bytes)

  def __eq__(self, o):
    return isinstance(o, Binary) and self.bytes == o.bytes or self.bytes == o

  def __repr__(self):
    return "Binary(%r)" % self.bytes

# XXX: sym instead of Symbol?

class Encoding:

  def __init__(self, name, type, code, category, width):
    self.name = name
    self.type = type
    self.code = code
    self.category = category
    self.width = width

  def __repr__(self):
    return "Encoding(%r, %r, %r, %r, %r)" % \
        (self.name, self.type, self.code, self.category, self.width)

def load_encodings(doc):
  result = []
  for enc in doc.query["amqp/section/type/encoding",
                       lambda n: n.parent["@class"] == "primitive"]:
    type_name = pythonize(enc.parent["@name"])
    if enc["@name"]:
      enc_name = "%s_%s" % (type_name, pythonize(enc["@name"]))
    else:
      enc_name = type_name
    code = int(enc["@code"], 0)
    category = pythonize(enc["@category"])
    width = int(enc["@width"], 0)
    result.append(Encoding(enc_name, Primitive(type_name), code, category, width))
  return result

TYPES = load_xml("types.xml")
ENCODINGS = load_encodings(TYPES)

WIDTH_CODES = {
  1: "B",
  2: "H",
  4: "I"
  }

class TypeEncoder:

  def __init__(self, encodings=ENCODINGS):
    self.encodings = {}
    self.encoders = {}
    for enc in encodings:
      if enc.type in self.encodings:
        orig = self.encodings[enc.type]
        if orig.width < enc.width:
          self.encodings[enc.type] = enc
      else:
        self.encodings[enc.type] = enc
      self.encoders[enc.type] = getattr(self, "enc_%s" % enc.type.name)
    self.types = {
      bool: Primitive("boolean"),
      int: Primitive("long"),  # the boundary between int and long is
      long: Primitive("long"), # platform specific, so we treat them the
                               # same to avoid platform dependencies
      float: Primitive("double"), # python floats are actually doubles
      datetime.datetime: Primitive("timestamp"),
      dict: Primitive("map"),
      list: Primitive("list"),
      tuple: Primitive("list"),
      Array: Primitive("array"),
      unicode: Primitive("string"),
      # XXX: the mapping choice here is a bit tricky given python's
      # blurring beween string and binary data
      str: Primitive("string"),
      Symbol: Primitive("symbol"),
      Binary: Primitive("binary"),
      uuid.UUID: Primitive("uuid"),
      None.__class__: Primitive("null")
      }
    self.deconstructors = {
      Value: self.deconstruct_value,
      }

  def deconstruct_value(self, v):
    return v.type, v.value

  def default_deconstructor(self, v):
    return self.types[v.__class__], v

  def deconstruct(self, value):
    deconstructor = self.deconstructors.get(value.__class__, self.default_deconstructor)
    return deconstructor(value)

  def encode(self, value):
    type, value = self.deconstruct(value)
    encoder, encoded, value = self.encode_type(type, value)
    return "%s%s" % (encoded, encoder(value))

  def encode_type(self, type, value):
    return type.encode(self, value)

  def encode_described(self, type, value):
    if type.source is None:
      source, value = self.deconstruct(value)
    else:
      source = type.source
    encoder, encoded, value = self.encode_type(source, value)
    return encoder, "\x00%s%s" % (self.encode(type.descriptor), encoded), value

  def encode_primitive(self, type, value):
    if type.name is None:
      type, value = self.deconstruct(value)
      return self.encode_type(type, value)
    else:
      enc = self.encodings[type]
      encoder = self.encoders[enc.type]
      return encoder, struct.pack("!B", enc.code), value

  def enc_null(self, n):
    return ""

  def enc_boolean(self, b):
    if b:
      return "\x01"
    else:
      return "\x00"

  def enc_ubyte(self, b):
    return struct.pack("!B", b)

  def enc_ushort(self, s):
    return struct.pack("!H", s)

  def enc_uint(self, i):
    return struct.pack("!I", i)

  def enc_ulong(self, l):
    return struct.pack("!Q", l)

  def enc_byte(self, b):
    return struct.pack("!b", b)

  def enc_short(self, s):
    return struct.pack("!h", s)

  def enc_int(self, i):
    return struct.pack("!i", i)

  def enc_long(self, l):
    return struct.pack("!q", l)

  def enc_float(self, f):
    return struct.pack("!f", f)

  def enc_double(self, d):
    return struct.pack("!d", d)

  def enc_decimal32(self, d):
    xxx

  def enc_decimal64(self, d):
    xxx

  def enc_decimal128(self, d):
    xxx

  def enc_char(self, c):
    return struct.pack("!I", ord(c))

  def enc_timestamp(self, t):
    return struct.pack("!q", 1000*int(time.mktime(t.timetuple())))

  def enc_uuid(self, u):
    return struct.pack("!16s", u.bytes)

  def enc_binary(self, b):
    if isinstance(b, Binary):
      b = b.bytes
    return struct.pack("!I", len(b)) + b

  def enc_string(self, s):
    bytes = s.encode("utf8")
    return self.enc_binary(bytes)

  def enc_symbol(self, s):
    if isinstance(s, basestring):
      bytes = s.encode("ascii")
    else:
      bytes = s.name.encode("ascii")
    return self.enc_binary(bytes)

  def enc_list(self, l):
    encoded = "".join([self.encode(x) for x in l])
    return self.enc_binary("%s%s" % (struct.pack("!I", len(l)), encoded))

  def enc_array(self, a):
    type = a.type
    encoder, etype, _ = self.encode_type(type, None)
    count = struct.pack("!I", len(a.values))
    # XXX: should check that deconstructed value matches array type & descriptor
    encoded = "".join([encoder(self.deconstruct(v)[-1]) for v in a.values])

    return self.enc_binary("%s%s%s" % (count, etype, encoded))
 
  def enc_map(self, m):
    pairs = []
    for pair in m.items():
      pairs.extend(pair)
    return self.enc_list(pairs)

class TypeDecoder:

  def __init__(self, encodings=ENCODINGS):
    self.encodings = {}
    for enc in encodings:
      self.encodings[enc.code] = (enc, getattr(self, "dec_%s" % enc.name))
    self.constructors = {}

  def construct(self, type, value):
    return type.construct(self, value)

  def construct_primitive(self, type, value):
    return value

  def construct_described(self, type, value):
    value = self.construct(type.source, value)
    cons = self.constructors.get(type.descriptor, Value)
    return cons(type, value)

  def decode(self, bytes):
    type, decoder, bytes = self.decode_type(bytes)
    value, bytes = decoder(bytes)
    return self.construct(type, value), bytes

  def decode_type(self, bytes):
    code, bytes = self.unpack("!B", bytes)
    if code == 0:
      descriptor, bytes = self.decode(bytes)
      source, decoder, bytes = self.decode_type(bytes)
      return Described(descriptor, source), decoder, bytes
    else:
      encoding, decoder = self.encodings[code]
      return encoding.type, decoder, bytes

  def unpack(self, format, bytes, constructor=identity):
    n = struct.calcsize(format)
    return constructor(*struct.unpack(format, bytes[:n])), bytes[n:]

  def dec_null(self, bytes):
    return None, bytes

  def dec_boolean(self, bytes):
    v, bytes = self.unpack("!B", bytes)
    return v != 0, bytes

  def dec_boolean_true(self, bytes):
    return True, bytes

  def dec_boolean_false(self, bytes):
    return False, bytes

  def dec_ubyte(self, bytes):
    return self.unpack("!B", bytes)

  def dec_ushort(self, bytes):
    return self.unpack("!H", bytes)

  def dec_uint(self, bytes):
    return self.unpack("!I", bytes)

  def dec_uint_uint0(self, bytes):
    return 0, bytes

  def dec_uint_smalluint(self, bytes):
    return self.unpack("!B", bytes)

  def dec_ulong(self, bytes):
    return self.unpack("!Q", bytes)

  def dec_ulong_ulong0(self, bytes):
    return 0, bytes

  def dec_ulong_smallulong(self, bytes):
    return self.unpack("!B", bytes)

  def dec_byte(self, bytes):
    return self.unpack("!b", bytes)

  def dec_short(self, bytes):
    return self.unpack("!h", bytes)

  def dec_int(self, bytes):
    return self.unpack("!i", bytes)

  def dec_int_smallint(self, bytes):
    return self.unpack("!b", bytes)

  def dec_long(self, bytes):
    return self.unpack("!q", bytes)

  def dec_long_smalllong(self, bytes):
    return self.unpack("!b", bytes)

  def dec_float_ieee_754(self, bytes):
    return self.unpack("!f", bytes)

  def dec_double_ieee_754(self, bytes):
    return self.unpack("!d", bytes)

  def dec_decimal32_ieee_754(self, bytes):
    xxx

  def dec_decimal64_ieee_754(self, bytes):
    xxx

  def dec_decimal128_ieee_754(self, bytes):
    xxx

  def dec_char_utf32(self, bytes):
    return self.unpack("!I", bytes, unichr)

  def dec_timestamp_ms64(self, bytes):
    ms, bytes = self.dec_long(bytes)
    return datetime.datetime.fromtimestamp(ms/1000.0), bytes

  def dec_uuid(self, bytes):
    return uuid.UUID(bytes=bytes[:16]), bytes[16:]

  def dec_variable(self, format, bytes, constructor=identity):
    size, bytes = self.unpack(format, bytes)
    return constructor(bytes[:size]), bytes[size:]

  def dec_binary_vbin8(self, bytes):
    return self.dec_variable("!B", bytes)

  def dec_binary_vbin32(self, bytes):
    return self.dec_variable("!I", bytes)

  def dec_string_str8_utf8(self, bytes):
    return self.dec_variable("!B", bytes, lambda x: x.decode("utf8"))

  def dec_string_str32_utf8(self, bytes):
    return self.dec_variable("!I", bytes, lambda x: x.decode("utf8"))

  def dec_symbol_sym8(self, bytes):
    return self.dec_variable("!B", bytes, lambda x: Symbol(str(x.decode("ascii"))))

  def dec_symbol_sym32(self, bytes):
    return self.dec_variable("!I", bytes, lambda x: Symbol(str(x.decode("ascii"))))

  def dec_compound(self, format, bytes, constructor=identity):
    (size, count), bytes = self.unpack(format, bytes, lambda s, c: (s, c))
    result = []
    while count > 0:
      value, bytes = self.decode(bytes)
      result.append(value)
      count -= 1
    return constructor(result), bytes

  def dec_list_list0(self, bytes):
    return [], bytes

  def dec_list_list8(self, bytes):
    return self.dec_compound("!BB", bytes)

  def dec_list_list32(self, bytes):
    return self.dec_compound("!II", bytes)

  def dec_array(self, format, bytes, constructor=identity):
    (size, count), bytes = self.unpack(format, bytes, lambda s, c: (s, c))
    type, decoder, bytes = self.decode_type(bytes)

    values = []
    while count > 0:
      element, bytes = decoder(bytes)
      values.append(self.construct(type, element))
      count -= 1

    return Array(type, values), bytes

  def dec_array_array8(self, bytes):
    return self.dec_array("!BB", bytes)

  def dec_array_array32(self, bytes):
    return self.dec_array("!II", bytes)

  def dec_map(self, elements):
    result = {}
    for k, v in zip(elements[::2], elements[1::2]):
      result[k] = v
    return result

  def dec_map_map8(self, bytes):
    return self.dec_compound("!BB", bytes, self.dec_map)

  def dec_map_map32(self, bytes):
    return self.dec_compound("!II", bytes, self.dec_map)
