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

import optparse, os, traceback
from client import *

def loop(link, obj):
  link.flow(100)
  while link.pending(block=True):
    msg = link.get()
    link.disposition(msg.delivery_tag, dispatch(msg, obj))
    if link.credit() < 50: link.flow(100)
    settle(link)

  settle(link)

  for tag, local, remote in self.lnk.get_unsettled():
    print "UNSETTLED:", tag, local, remote

def settle(link):
  for tag, _, _ in link.get_remote(settled=True):
    link.settle(tag)

def dispatch(msg, obj):
  try:
    opcode = msg["opcode"].replace("-", "_")
  except KeyError:
    print "malformed message:", msg
    return ACCEPTED
  else:
    try:
      return getattr(obj, opcode, getattr(obj, "unknown", unknown))(msg)
    except:
      print "error processing message:", msg
      traceback.print_exc()
      return ACCEPTED

def unknown(msg):
  print "unknown opcode:", msg
  return ACCEPTED

def options():
  parser = optparse.OptionParser(usage="usage: %prog [options] <address>",
                                 description="receive messages")
  parser.add_option("-H", "--host",
                    help="host to connect to (default 0.0.0.0)")
  parser.add_option("-p", "--port", type=int, default=5672,
                    help="port to connect to (default %default)")
  parser.add_option("-u", "--username", default="demo",
                    help="username to use for authentication")
  parser.add_option("-w", "--password", default="demo",
                    help="password to use for authentication")
  parser.add_option("-t", "--trace", default="err",
                    help="enable tracing for specified categories")

  return parser.parse_args()

def main(cls, source):
  opts, args = options()

  if args:
    parser.error("unrecognized arguments")

  conn = Connection(auth=True)
  conn.tracing(*opts.trace.split())
  conn.connect(opts.host or os.getenv('AMQP_BROKER') or "0.0.0.0", opts.port)
  conn.open(mechanism="PLAIN", username=opts.username, password=opts.password)
  ssn = conn.session()
  lnk = ssn.receiver(source, limit=100)
  try:
    loop(lnk, cls(ssn, lnk))
  except KeyboardInterrupt:
    pass
  lnk.close()
  ssn.close()
  conn.close()
