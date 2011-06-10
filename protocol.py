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

from codec import TYPES as TYPES_DOC, TypeDecoder, TypeEncoder, Symbol, UNDESCRIBED
from composite import Composite, load_composite, Field
from util import load_xml, pythonize

class Body(Composite):
  FIELDS=[Field("payload", Symbol("payload"), "binary", "binary", UNDESCRIBED,
                False, False, None, "")]

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
CLASSES = load_composite(TYPES, Composite, frame=Body, sasl_frame=Body,
                         outcome=FieldCompare, TransferState=FieldCompare)

PROTOCOL_DECODER = TypeDecoder()
PROTOCOL_ENCODER = TypeEncoder()

for cls in CLASSES:
  PROTOCOL_ENCODER.deconstructors[cls] = lambda v: (v.DESCRIPTORS[0],
                                                    v.SOURCE,
                                                    v.deconstruct())
  for d in cls.DESCRIPTORS:
    if cls.SOURCE == "map":
      const = lambda t, m, d, c=cls: c(**dict([(pythonize(k.name), v)
                                               for (k, v) in m.iteritems()]))
    else:
      const = lambda t, l, d, c=cls: c(*l)
    PROTOCOL_DECODER.constructors[d] = const

__all__ = ["CLASSES", "PROTOCOL_DECODER", "PROTOCOL_ENCODER"]

for cls in CLASSES:
  globals()[cls.__name__] = cls
  __all__.append(cls.__name__)

ACCEPTED = DeliveryState(outcome=Accepted())
REJECTED = DeliveryState(outcome=Rejected())
RELEASED = DeliveryState(outcome=Released())
