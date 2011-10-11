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

def loop(link, obj, batch=False, timeout=None):
  link.flow(100)
  while True:
    try:
      more = link.pending(block=True, timeout=timeout)
    except Timeout:
      try:
        obj.idle()
      except:
        print "error idling"
        traceback.print_exc()
      continue

    if more:
      msg = link.get()
      link.disposition(msg.delivery_tag, dispatch(msg, obj))
      if link.credit() < 50: link.flow(100)
      settle(link)
      if batch:
        if not link.pending():
          try:
            obj.process()
          except:
            print "error processing"
            traceback.print_exc()

  settle(link)

  for tag, local, remote in self.lnk.get_unsettled():
    print "UNSETTLED:", tag, local, remote

def settle(link):
  for tag, l, r in link.get_remote(settled=True):
    if l.state is not None:
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
  parser = optparse.OptionParser(usage="usage: %prog [options]")
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
  return parser

def open_conn(opts):
  conn = Connection(auth=True)
  conn.tracing(*opts.trace.split())
  conn.connect(opts.host or os.getenv('AMQP_BROKER') or "0.0.0.0", opts.port)
  conn.open(mechanism="PLAIN", username=opts.username, password=opts.password)
  return conn

def main(cls, source):
  parser = options()
  opts, args = parser.parse_args()

  if args:
    parser.error("unrecognized arguments")

  conn = open_conn(opts)
  ssn = conn.session()
  lnk = ssn.receiver(source, limit=100)
  try:
    loop(lnk, cls(ssn, lnk))
  except KeyboardInterrupt:
    pass
  lnk.close()
  ssn.close()
  conn.close()

RH = "redhat"
VM = "vmware"
IN = "inetco"
AP = "apache"
MS = "microsoft"
SW = "swiftmq"

VENDORS = [RH, VM, IN, AP, MS, SW]

CLIENTS = [RH, AP, SW, IN, MS]
BROKERS = [RH, VM, IN, AP, SW, MS]
BRADDRS = {
  RH: ("ec2-50-16-33-219.compute-1.amazonaws.com", 5672, "demo", "demo"),
  VM: ("ec2-79-125-83-217.eu-west-1.compute.amazonaws.com", 5672, "guest", "guest"),
  IN: ("insight.inetco.com", 5672, "demo", "demo"),
  AP: ("ec2-50-16-33-219.compute-1.amazonaws.com", 15672, "guest", "guest"),
  SW: ("mail.iit.de", 5672, "demo", "demo"),
  MS: ("int7001sbuser-0-9.servicebus.int7.windows-int.net", 5672, "user", "Passw0rd!12")
  }

def reset(target="*"):
  msg = Message()
  msg["opcode"] = "reset"
  msg["target"] = target
  return msg

def create_link(address, role="sender", ref=None, host="0.0.0.0", port="5672",
                user="demo", password="demo", target="*"):
  msg = Message()
  msg["opcode"] = "create-link"
  msg["address"] = address
  msg["role"] = role
  msg["link-ref"] = ref or "%s.%s" % (address, role)
  msg["host"] = host
  msg["port"] = port
  msg["sasl-user"] = user
  msg["sasl-password"] = password
  msg["target"] = target
  return msg

def send_message(ref, id, target="*"):
  msg = Message(content=u"AMQP-%s" % id)
  msg["opcode"] = "send-message"
  msg["link-ref"] = ref
  msg["message-id"] = id
  msg["target"] = target
  return msg

OPCODES = {
  "create-link": create_link,
  "send-message": send_message,
  "reset": reset
  }
