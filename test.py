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

import urlparse
from codec import TypeEncoder, TypeDecoder

class Location:

  def __init__(self, longitude, latitude):
    self.longitude = longitude
    self.latitude = latitude

  def __repr__(self):
    return "Location(%r, %r)" % (self.longitude, self.latitude)

enc = TypeEncoder()
enc.deconstructors[urlparse.ParseResult] = lambda u: ("url", urlparse.urlunparse(u))
enc.deconstructors[Location] = lambda l: ("location", (l.longitude, l.latitude))


dec = TypeDecoder()
dec.constructors["url"] = lambda d, v: urlparse.urlparse(v)
dec.constructors["location"] = lambda d, v: Location(*v)

generic_dec = TypeDecoder()


l = Location(1.2, 3.4)
print "Location:", l
bytes = enc.encode(l)
print "Encoded:", repr(bytes)
print "Decoded:", dec.decode(bytes)[0], generic_dec.decode(bytes)[0]

print

u = urlparse.urlparse(u"http://www.amqp.org")
print "URL:", u
bytes = enc.encode(u)
print "Encoded:", repr(bytes)
print "Decoded:", dec.decode(bytes)[0], generic_dec.decode(bytes)[0]

print

from framing import FrameDecoder, FrameEncoder, ConnectionFrame, SessionFrame

frenc = FrameEncoder()
frenc.write(ConnectionFrame(0, 0, "frame1 body"))
frenc.write(SessionFrame(0, 0, 0, 0, 0, 0, "frame2 body"))
frenc.write(ConnectionFrame(0, 0, "frame3 body"))
bytes = frenc.read()
print "Encoded Frames:", repr(bytes)

frdec = FrameDecoder()
frdec.write(bytes)
print "Frames:", frdec.read()

for b in bytes:
  frdec.write(b)
  frames = frdec.read()
  if frames:
    print "Frames:", frames
