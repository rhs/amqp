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

from codec import TYPES as TYPES_DOC, TypeDecoder, TypeEncoder, Symbol, \
    Described, Primitive, Binary
from composite import Composite, Restricted, load_composite, Field
from util import load_xml, pythonize

class Body(Composite):
  FIELDS=[Field("payload", Symbol("payload"), Primitive("binary"), False, False,
                "")]

class FieldCompare(Composite):

  def __eq__(self, o):
    if self.__class__ != o.__class__:
      return False

    for f in self.FIELDS:
      if getattr(self, f.name) != getattr(o, f.name):
        return False

    return True

TRANSPORT = load_xml("transport.xml")
MESSAGING = load_xml("messaging.xml")
SECURITY = load_xml("security.xml")
TRANSACTIONS = load_xml("transactions.xml")

TYPES = reduce(lambda x, y: x + y,
               [d.query["amqp/section/type"]
                for d in [TYPES_DOC, TRANSPORT, MESSAGING, SECURITY,
                          TRANSACTIONS]])
CLASSES = load_composite(TYPES, composite=Composite, restricted=Restricted,
                         frame=Body, sasl_frame=Body,
                         delivery_state=FieldCompare)

PROTOCOL_DECODER = TypeDecoder()
PROTOCOL_ENCODER = TypeEncoder()

for cls in CLASSES:
  PROTOCOL_ENCODER.deconstructors[cls] = lambda v: (v.TYPE, v.deconstruct())
  for d in cls.DESCRIPTORS:
    PROTOCOL_DECODER.constructors[d] = cls.construct

__all__ = ["CLASSES", "PROTOCOL_DECODER", "PROTOCOL_ENCODER", "Symbol",
           "Binary"]

for cls in CLASSES:
  globals()[cls.__name__] = cls
  __all__.append(cls.__name__)

ACCEPTED = Accepted()
REJECTED = Rejected()
RELEASED = Released()
