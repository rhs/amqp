import sys, time, common
from math import pi
from window import Window
from cairo import ImageSurface, LINE_CAP_ROUND
from common import *

LOGOS = {}
MESSAGES = {}
for v in VENDORS:
  LOGOS[v] = ImageSurface.create_from_png("images/broker-%s.png" % v)
for v in CLIENTS:
  MESSAGES[v] = ImageSurface.create_from_png("images/message-%s.png" % v)

SCREEN_H = 1.0
SCREEN_W = 16.0/9.0
BALL_SIZE = 0.1
VEND_SIZE = 0.25
VEND_W = VEND_SIZE
VEND_H = VEND_SIZE

class Ball:

  def __init__(self, vendor):
    self.vendor = vendor
    self.x = 0.0
    self.y = 0.0
    self.angle = 0.0
    self.alpha = 1.0

  def draw(self, cr, w, h):
    scale = BALL_SIZE
    cr.translate(self.x, self.y)
    cr.rotate(self.angle)
    cr.translate(-scale/2, scale/2)
    ball = MESSAGES[self.vendor]
    cr.scale(scale/ball.get_width(), -scale/ball.get_height())
    cr.set_source_surface(ball)
    cr.paint_with_alpha(self.alpha)

def component(cr, vendor):
  logo = LOGOS[vendor]
  cr.translate(0.0, 1.0)
  cr.scale(1.0/logo.get_width(), -1.0/logo.get_height())
  cr.set_source_surface(logo)
  cr.paint()

class Broker:

  client = False
  broker = True

  def __init__(self, vendor):
    self.vendor = vendor
    self.messages = []

  def enqueue(self, item):
    self.messages.append(item)

  def dequeue(self):
    return self.messages.pop(0)

  def draw(self, cr, w, h):
    cr.save()
    component(cr, self.vendor)
    cr.restore()
    if self.messages:
      cr.translate((1.0 - BALL_SIZE/VEND_W)/2, 0.55 + 0.5*BALL_SIZE/VEND_H)
      for i in range(len(self.messages)):
        msg = MESSAGES[self.messages[len(self.messages) - 1 - i]]
        cr.save()
        cr.translate(0, 0.1*(i - len(self.messages)/2.0))
        cr.scale(BALL_SIZE/(VEND_W*msg.get_width()), -BALL_SIZE/(VEND_H*msg.get_height()))
        cr.set_source_surface(msg)
        cr.paint()
        cr.restore()

class Client:

  client = True
  broker = False

  def __init__(self, vendor):
    self.vendor = vendor

  def draw(self, cr, w, h):
    component(cr, self.vendor)

class Line:

  def __init__(self, x1, y1, x2, y2, width=0.05):
    self.x1 = x1
    self.y1 = y1
    self.x2 = x2
    self.y2 = y2

  def draw(self, cr, w, h):
    cr.set_source_rgb(0.9, 0.9, 0.9)
    cr.set_line_width(0.01)
    cr.set_line_cap(LINE_CAP_ROUND)
    cr.move_to(self.x1, self.y1)
    cr.line_to(self.x2, self.y2)
    cr.stroke()
    cr.set_source_rgb(0.5, 0.5, 0.5)
    cr.set_line_width(0.005)
    cr.move_to(self.x1 + 0.0001, self.y1)
    cr.line_to(self.x2 - 0.0001, self.y2)
    cr.stroke()

class Text:

  def __init__(self, text):
    self.text = text

  def draw(self, cr, w, h):
    cr.translate(-0.5, 0)
    cr.set_source_rgb(0.75, 0.75, 0.75)
    cr.set_font_size(1.0)
    xb, yb, w, h, xa, ya = cr.text_extents(self.text)
    cr.scale(1.0/w, -1.0/h)
    cr.show_text(self.text)
    cr.stroke()

BRKS = {
  RH: Broker(RH),
  VM: Broker(VM),
  MS: Broker(MS),
  IN: Broker(IN),
  AP: Broker(AP),
  SW: Broker(SW)
  }
CLIS = {
  RH: Client(RH),
  MS: Client(MS),
  IN: Client(IN),
  AP: Client(AP),
  SW: Client(SW)
  }

window = Window()
cpad = (SCREEN_W - VEND_W*len(CLIS))/(len(CLIS)+1)
window.add(Text("Clients"), SCREEN_W/2, 0.9, 0.2, 0.05)
idx = 0
for c in (RH, MS, IN, AP, SW):
  window.add(CLIS[c], cpad*(idx+1) + VEND_W*idx, 0.9 - VEND_H, VEND_W, VEND_H)
  idx += 1

window.add(Line(0.0375, 0.5, SCREEN_W - 0.0375, 0.5), 0, 0, 1, 1)

window.add(Text("Brokers"), SCREEN_W/2, 0.05, 0.2, 0.05)
bpad = (SCREEN_W - VEND_W*len(BRKS))/(len(BRKS)+1)
idx = 0
for c in (RH, VM, MS, IN, AP, SW):
  window.add(BRKS[c], bpad*(idx+1) + VEND_W*idx, 0.1, VEND_W, VEND_H)
  idx += 1

ROOT = {
  "C": CLIS,
  "B": BRKS
  }

def lookup(path):
  cls, vnd = path.split(":")
  return ROOT[cls][getattr(common, vnd)]

for i in range(250):
  window.redraw()

fade = 100
steps = 300

class Path:

  def __init__(self, ball, start, stop):
    self.ball = ball
    self.start = start
    self.stop = stop

def center(w):
  return (w._Screen__x + w._Screen__w/2, w._Screen__y + w._Screen__h/2)

def go(paths):
  paths.reverse()
  for p in paths:
    p.ball.x, p.ball.y = center(p.start)
    if p.start.client:
      p.ball.alpha = 0.0
    window.add(p.ball, 0, 0, 1.0, 1.0)
  paths.reverse()

  # fade in and dequeue
  for p in paths:
    if p.start.client:
      for i in range(fade + 1):
        p.ball.alpha = i/float(fade)
        window.redraw()
    elif p.start.broker:
      p.ball.vendor = p.start.dequeue()

  # travel
  for i in range(steps+1):
    for p in paths:
      x1, y1 = center(p.start)
      x2, y2 = center(p.stop)
      dx = x2 - x1
      dy = y2 - y1
      p.ball.x = x1 + dx*float(i)/steps
      p.ball.y = y1 + dy*float(i)/steps
      p.ball.angle = 2*pi*float(i)/steps
    window.redraw()

  # fade out and enqueue
  for p in paths:
    if p.stop.broker:
      p.stop.enqueue(p.ball.vendor)
      window.redraw()
    elif p.stop.client:
      for i in range(fade + 1):
        p.ball.alpha = 1.0 - i/float(fade)
        window.redraw()
    window.remove(p.ball)
  window.redraw()

def script(fname):
  paths = []

  for line in open(fname):
    if line.strip() == "--":
      go(paths)
      paths = []
    elif line.strip().startswith("+"):
      time.sleep(float(line.strip()[1:]))
    else:
      start, stop = line.split()
      s = lookup(start)
      paths.append(Path(Ball(s.vendor), lookup(start), lookup(stop)))

  time.sleep(1)

class Listener:

  def __init__(self, ssn, lnk):
    pass

  def log(self, msg):
    print "LOG:", msg
    action = msg["action"]
    vendor = msg["vendor"]
    cli = CLIS[vendor]
    brkv, num = msg["message-id"].split(":")
    brk = BRKS[brkv]
    if action == "sent":
      start = cli
      stop = brk
    else:
      start = brk
      stop = cli
    paths = [Path(Ball(start.vendor), start, stop)]
    go(paths)

def listen():
  main(Listener, "log")

script("sequence")
#listen()
