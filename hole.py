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

from messaging import Message, encode
from protocol import ACCEPTED, Transfer

class Hole:

  def __init__(self):
    self.next_id = 0

  def identify(self):
    id = self.next_id
    self.next_id += 1
    return id

  def capacity(self):
    return True

  def source(self):
    return Source(self)

  def target(self):
    return Target(self)

class Source:

  def __init__(self, hole):
    self.hole = hole

  def get(self):
    return str(self.hole.identify()), Transfer(fragments=encode(Message()))

  def resume(self, unsettled):
    pass

  def settle(self, tag, state):
    pass

  def close(self):
    pass

class Target:

  def __init__(self, hole):
    self.hole = hole

  def capacity(self):
    return self.hole.capacity()

  def put(self, tag, message):
    pass

  def resume(self, unsettled):
    pass

  def settle(self, tag):
    return ACCEPTED

  def close(self):
    pass
