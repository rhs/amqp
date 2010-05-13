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

import struct, uuid, cStringIO
from util import pythonize, load_xml, identity

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
    result.append(Encoding(enc_name, type_name, code, category, width))
  return result

TYPES = load_xml("types.xml")
ENCODINGS = load_encodings(TYPES)

WIDTH_CODES = {
  1: "B",
  2: "H",
  4: "I"
  }

class Box:

  def __init__(self, type, value):
    self.type = type
    self.value = value

  def __repr__(self):
    return "Box(%r, %r)" % (self.type, self.value)

class Described:

  def __init__(self, descriptor, value):
    self.descriptor = descriptor
    self.value = value

  def __repr__(self):
    return "Described(%r, %r)" % (self.descriptor, self.value)

class Symbol:

  def __init__(self, name):
    self.name = name

  def __hash__(self):
    return hash(self.name)

  def __eq__(self, o):
    return self.name == o.name

  def __repr__(self):
    return "Symbol(%r)" % self.name

UNDESCRIBED = object()

class TypeEncoder:

  def __init__(self, encodings=ENCODINGS):
    self.encodings = {}
    for enc in encodings:
      self.encodings[enc.name] = enc
      if not (hasattr(self, "enc_%s" % enc.type)):
        raise ValueError("no encoder for encoding: %s" % enc)
    self.encoders = {
      Box: self.enc_box,
      bool: self.enc_boolean,
      int: self.enc_long, # the boundary between int and long is
                          # platform specific, so we treat them the
                          # same to avoid platform dependencies
      long: self.enc_long,
      float: self.enc_double, # python floats are actually doubles
      dict: self.enc_map,
      list: self.enc_list,
      tuple: self.enc_list,
      unicode: self.enc_string,
      # XXX: the mapping choice here is a bit tricky given python's
      # blurring beween string and binary data
      str: self.enc_string,
      Symbol: self.enc_symbol,
      buffer: self.enc_binary,
      uuid.UUID: self.enc_uuid,
      None.__class__: self.enc_null
      }
    self.deconstructors = {
      Described: lambda v: (v.descriptor, v.value)
      }

  def deconstruct(self, value):
    deconstructor = self.deconstructors.get(value.__class__, lambda v: (UNDESCRIBED, v))
    descriptor, value = deconstructor(value)
    encoder = self.encoders[value.__class__]
    return descriptor, encoder, value

  def encode(self, value):
    descriptor, encoder, value = self.deconstruct(value)
    if descriptor is UNDESCRIBED:
      return encoder(value)
    else:
      return "\x00%s%s" % (self.encode(descriptor), self.encode(value))

  def enc_fixed(self, enc_name, format=None, *args):
    enc = self.encodings[enc_name]
    if format:
      bytes = struct.pack(format, *args)
    else:
      bytes = ""
    assert len(bytes) == enc.width
    return struct.pack("!B", enc.code) + bytes

  def enc_box(self, b):
    encoder = getattr(self, "enc_%s" % b.type, None)
    if encoder:
      return encoder(b.value)
    else:
      raise ValueError("unknown type: %s" % b.type)

  def enc_null(self, n):
    return self.enc_fixed("null")

  def enc_boolean(self, b):
    if b:
      return self.enc_fixed("boolean_true")
    else:
      return self.enc_fixed("boolean_false")

  def enc_ubyte(self, b):
    return self.enc_fixed("ubyte", "!B", b)

  def enc_ushort(self, s):
    return self.enc_fixed("ushort", "!H", s)

  def enc_uint(self, i):
    return self.enc_fixed("uint", "!I", i)

  def enc_ulong(self, l):
    return self.enc_fixed("ulong", "!Q", l)

  def enc_byte(self, b):
    return self.enc_fixed("byte", "!b", b)

  def enc_short(self, s):
    return self.enc_fixed("short", "!h", s)

  def enc_int(self, i):
    return self.enc_fixed("int", "!i", i)

  def enc_long(self, l):
    return self.enc_fixed("long", "!q", l)

  def enc_float(self, f):
    return self.enc_fixed("float_ieee_754", "!f", f)

  def enc_double(self, d):
    return self.enc_fixed("double_ieee_754", "!d", d)

  def enc_char(self, c):
    return self.enc_fixed("char_utf32", "!I", ord(c))

  def enc_timestamp(self, t):
    xxx

  def enc_uuid(self, u):
    return self.enc_fixed("uuid", "!16s", u.bytes)

  def enc_variable(self, base_enc, bytes):
    if len(bytes) < 256:
      format = "!B"
      enc = "%s8" % base_enc
    else:
      format = "!I"
      enc = "%s32" % base_enc
    return self.enc_fixed(enc, format, len(bytes)) + bytes

  def enc_binary(self, b):
    if isinstance(b, buffer):
      b = str(b)
    return self.enc_variable("binary_vbin", b)

  def enc_string(self, s):
    bytes = s.encode("utf8")
    return self.enc_variable("string_str8_utf", bytes)

  def enc_symbol(self, s):
    bytes = s.name.encode("ascii")
    return self.enc_variable("symbol_sym", bytes)

  def enc_compound(self, base_enc, elements):
    encoded = "".join([self.encode(x) for x in elements])
    count = len(elements)
    # XXX: this is sort of a PITA to get right
    if count < 256 and len(encoded) < 255:
      encoded = struct.pack("!B", count) + encoded
    else:
      encoded = struct.pack("!I", count) + encoded
    return self.enc_variable(base_enc, encoded)

  def enc_list(self, l):
    return self.enc_compound("list_list", l)

  def enc_map(self, m):
    elements = []
    for pair in m.items():
      elements.extend(pair)
    return self.enc_compound("map_map", elements)

class TypeDecoder:

  def __init__(self, encodings=ENCODINGS):
    self.decoders = {}
    for enc in encodings:
      self.decoders[enc.code] = (enc, getattr(self, "dec_%s" % enc.name))
    self.constructors = {
      UNDESCRIBED: lambda d, v: v
      }

  def construct(self, descriptor, value):
    constructor = self.constructors.get(descriptor, Described)
    return constructor(descriptor, value)

  def decode(self, bytes):
    descriptor, (encoding, decoder), bytes = self.dec_type(bytes)
    value, bytes = decoder(bytes)
    return self.construct(descriptor, value), bytes

  def unpack(self, format, bytes, constructor=identity):
    n = struct.calcsize(format)
    return constructor(*struct.unpack(format, bytes[:n])), bytes[n:]

  def dec_type(self, bytes):
    code, bytes = self.unpack("!B", bytes)
    if code == 0:
      descriptor, bytes = self.decode(bytes)
      code, bytes = self.unpack("!B", bytes)
    else:
      descriptor = UNDESCRIBED
    return descriptor, self.decoders[code], bytes

  def dec_null(self, bytes):
    return None, bytes

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

  def dec_ulong(self, bytes):
    return self.unpack("!Q", bytes)

  def dec_byte(self, bytes):
    return self.unpack("!b", bytes)

  def dec_short(self, bytes):
    return self.unpack("!h", bytes)

  def dec_int(self, bytes):
    return self.unpack("!i", bytes)

  def dec_long(self, bytes):
    return self.unpack("!q", bytes)

  def dec_float_ieee_754(self, bytes):
    return self.unpack("!f", bytes)

  def dec_double_ieee_754(self, bytes):
    return self.unpack("!d", bytes)

  def dec_char_utf32(self, bytes):
    return self.unpack("!I", bytes, unichr)

  def dec_timestamp_ms64(self, t):
    xxx

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

  def dec_string_str8_utf16(self, bytes):
    return self.dec_variable("!B", bytes, lambda x: x.decode("utf16"))

  def dec_string_str32_utf16(self, bytes):
    return self.dec_variable("!I", bytes, lambda x: x.decode("utf16"))

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

  def dec_list_list8(self, bytes):
    return self.dec_compound("!BB", bytes)

  def dec_list_list32(self, bytes):
    return self.dec_compound("!II", bytes)

  def dec_list_array8(self, bytes):
    xxx

  def dec_list_array32(self, bytes):
    xxx

  def dec_map(self, elements):
    result = {}
    for k, v in zip(elements[::2], elements[1::2]):
      result[k] = v
    return result

  def dec_map_map8(self, bytes):
    return self.dec_compound("!BB", bytes, self.dec_map)

  def dec_map_map32(self, bytes):
    return self.dec_compound("!II", bytes, self.dec_map)
