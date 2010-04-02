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

def connection():
  from connection import Connection
  from session import Session
  from link import Sender, Receiver, link
  from operations import Fragment

  a = Connection(lambda n: Session(n, link))
  b = Connection(lambda n: Session(n, link))

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
  s.attach()
  s2.attach()
  s.detach(False)
  s.attach()
  l = Sender("qwer", source="S", target="T")
  s.add(l)
  l.link()

  pump()

  bssn = b.sessions["test-ssn"]
  bssn.attach()
  bl = bssn.links["qwer"]
  bl.link()
  bl.flow(10)

  pump()

  l.settle(l.send(fragments=Fragment(True, True, 0, 0, "asdf")))
  tag = l.send(delivery_tag="blah", fragments=Fragment(True, True, 0, 0, "asdf"))

  pump()

  ln = bssn.links["qwer"]
  x = ln.get()
  print "INCOMING XFR:", x
  ln.ack(x)

  xfr = ln.get()
  print "INCOMING XFR:", xfr
  ln.ack(xfr, key="value")

  print "--"

  pump()

  print "--"

  print "DISPOSITION", l.get()
  l.settle(tag)

  l.unlink()
  bl.unlink()

  pump()

  s.detach(True)
  pump()
  bssn.detach(True)
  s2.detach(True)
  a.close()
  b.close()

  pump()

connection()

def session():
  from connection import Connection
  from session import Session
  from link import link, Sender, Receiver

  a = Connection(None)
  a.tracing = set(["ops", "err"])
  a.id = "A"
  b = Connection(None)
  b.tracing = set(["err"])
  b.id = "B"
  ssn = Session("test", link)
  a.add(ssn)
  nss = Session("test", link)
  b.add(nss)

  def pump():
    a.tick()
    b.tick()
    b.write(a.read())
    a.write(b.read())

  ssn.attach()
  nss.attach()

  snd = Sender("L", "S", "T")
  ssn.add(snd)
  rcv = Receiver("L", "S", "T")
  nss.add(rcv)

  snd.link()
  rcv.link()
  rcv.flow(10)

  pump()

  from operations import Fragment
  snd.send(fragments=Fragment(True, True, 0, 0, "m1"))
  snd.send(fragments=Fragment(True, True, 0, 0, "m2"))
  dt3 = snd.send(fragments=Fragment(True, True, 0, 0, "m3"))

  pump()

  print rcv.pending()

  rcv.flow(0)

  pump()

  snd.send(fragments=Fragment(True, True, 0, 0, "m4"))

  pump()

  xfrs = []
  while rcv.pending():
    x = rcv.get()
    xfrs.append(x)
    print "XFR", x

  rcv.ack(xfrs[-1])

  rcv.flow(0)

  pump()

  snd.send(fragments=Fragment(True, True, 0, 0, "m5"))

  pump()

  print nss.exe_deferred

  rcv.ack(xfrs[0])

  rcv.flow(0)

  pump()

  print ssn.acknowledged, ssn.ack_deferred

  snd.settle(dt3)

  print ssn.acknowledged, ssn.ack_deferred

  for dt in list(snd.unsettled):
    snd.settle(dt)

  snd.unlink()
  rcv.unlink()

  pump()

  print ssn.acknowledged, ssn.ack_deferred, ssn.on_exe

print "=========="
session()
