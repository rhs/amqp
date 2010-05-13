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

from codec import TYPES as TYPES_DOC
from compound import Compound, load_compound
from util import load_xml

class Body(Compound):
  pass

TRANSPORT = load_xml("transport.xml")
TYPES = TYPES_DOC.query["amqp/section/type"] + \
    TRANSPORT.query["amqp/section/type"]
CLASSES = load_compound(TYPES, Compound, frame=Body)

__all__ = ["CLASSES"]

for cls in CLASSES:
  globals()[cls.__name__] = cls
  __all__.append(cls.__name__)
