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

from framing import Frame, FrameDecoder, FrameEncoder

frenc = FrameEncoder()
frenc.write(Frame(0, 1, None, "frame1 body"))
frenc.write(Frame(0, 2, "extended header", "frame2 body"))
frenc.write(Frame(1, 0, "e", "frame3 body"))
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

def connection():
  from connection import Connection
  from session import Session
  from link import Sender, Receiver, link
  from protocol import Fragment, Linkage

  a = Connection(lambda n: Session(n, link))
  b = Connection(lambda n: Session(n, link))
  a.id = "A"
  a.tracing = set(["ops", "err"])
  b.id = "B"
  b.tracing = set(["ops", "err"])

  def pump():
    while a.pending() or b.pending():
      b.write(a.read())
      a.write(b.read())

  s = Session("test-ssn", link)
  a.add(s)
  s2 = Session("test-ssn2", link)
  a.add(s2)

  a.open(hostname="asdf")
  b.open()
  s.begin()
  s2.begin()
  l = Sender("qwer", local=Linkage("S", "T"))
  s.add(l)
  l.attach()

  pump()

  bssn = [x for x in b.incoming.values() if x.name == "test-ssn"][0]
  bssn.begin()
  bl = bssn.links["qwer"]
  bl.attach()
  bl.flow(10)

  pump()

  l.settle(l.send(fragments=Fragment(True, True, 0, 0, "asdf")))
  tag = l.send(delivery_tag="blah", fragments=Fragment(True, True, 0, 0, "asdf"))

  pump()

  ln = bssn.links["qwer"]
  x = ln.get()
  print "INCOMING XFR:", x
  ln.disposition(x.delivery_tag, "ACCEPTED")

  xfr = ln.get()
  print "INCOMING XFR:", xfr
  ln.disposition(xfr.delivery_tag, "ASDF")

  print "--"

  pump()

  print "--"

  print "DISPOSITION", l.get_remote(modified=True)
  l.settle(tag)

  l.detach()
  bl.detach()

  pump()

  s.end(True)
  pump()
  bssn.end(True)
  s2.end(True)
  a.close()
  b.close()

  pump()

connection()

def session():
  from connection import Connection
  from session import Session
  from link import link, Sender, Receiver
  from protocol import Fragment, Linkage

  a = Connection(lambda n: Session(n, link))
  a.tracing = set(["ops", "err"])
  a.id = "A"
  b = Connection(lambda n: Session(n, link))
  b.tracing = set(["err"])
  b.id = "B"
  ssn = Session("test", link)
  a.add(ssn)
  ssn.begin()
#  nss = Session("test", link)
#  b.add(nss)

  def pump():
    a.tick()
    b.tick()
    b.write(a.read())
    a.write(b.read())

  pump()

  nss = [s for s in b.incoming.values() if s.name == "test"][0]
  nss.begin()

  snd = Sender("L", "S", "T")
  ssn.add(snd)
  rcv = Receiver("L", "S", "T")
  nss.add(rcv)

  snd.attach()
  rcv.attach()
  rcv.flow(10)

  pump()

  snd.send(fragments=Fragment(True, True, 0, 0, "m1"))
  snd.send(fragments=Fragment(True, True, 0, 0, "m2"))
  dt3 = snd.send(fragments=Fragment(True, True, 0, 0, "m3"))

  pump()

  print "PENDING", rcv.pending()

  pump()

  snd.send(fragments=Fragment(True, True, 0, 0, "m4"))

  pump()

  xfrs = []
  while rcv.pending():
    x = rcv.get()
    xfrs.append(x)
    print "XFR", x

  rcv.disposition(xfrs[-1].delivery_tag, "ACCEPTED")

  pump()

  snd.send(fragments=Fragment(True, True, 0, 0, "m5"))

  pump()

  rcv.disposition(xfrs[0].delivery_tag, "ACCEPTED")

  print "----------"
  pump()
  print "----------"

  print "ssn.outgoing:", ssn.outgoing
  print "snd.unsettled:", snd.unsettled
  for xfr in xfrs[1:-1]:
    rcv.disposition(xfr.delivery_tag, "ACCEPTED")
  print "rcv.unsettled", rcv.unsettled
  print "rcv.pending()", rcv.pending()
  rcv.disposition(rcv.get().delivery_tag)

  pump()
  print "----------"

  print "ssn.outgoing:", ssn.outgoing
  print "snd.unsettled:", snd.unsettled

  print "settling:", dt3
  snd.settle(dt3)

  print "ssn.outgoing:", ssn.outgoing
  print "snd.unsettled:", snd.unsettled

  for dt in list(snd.unsettled):
    snd.settle(dt)

  snd.detach()
  rcv.detach()

  pump()

  print "ssn.outgoing:", ssn.outgoing
  print "snd.unsettled:", snd.unsettled

print "=========="
session()
