import sys, time
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
  "RH": Broker(RH),
  "VM": Broker(VM),
  "MS": Broker(MS),
  "IN": Broker(IN),
  "AP": Broker(AP),
  "SW": Broker(SW)
  }
CLIS = {
  "RH": Client(RH),
  "MS": Client(MS),
  "IN": Client(IN),
  "AP": Client(AP),
  "SW": Client(SW)
  }

window = Window()
cpad = (1.7 - VEND_W*len(CLIS))/(len(CLIS)+1)
window.add(Text("Clients"), 1.7/2, 0.9, 0.2, 0.05)
idx = 0
for c in ("RH", "MS", "IN", "AP", "SW"):
  window.add(CLIS[c], cpad*(idx+1) + VEND_W*idx, 0.9 - VEND_H, VEND_W, VEND_H)
  idx += 1

window.add(Line(0.0375, 0.5, 1.7 - 0.0375, 0.5), 0, 0, 1, 1)

window.add(Text("Brokers"), 1.7/2, 0.05, 0.2, 0.05)
bpad = (1.7 - VEND_W*len(BRKS))/(len(BRKS)+1)
idx = 0
for c in ("RH", "VM", "MS", "IN", "AP", "SW"):
  window.add(BRKS[c], bpad*(idx+1) + VEND_W*idx, 0.1, VEND_W, VEND_H)
  idx += 1

CLI_COORDS = {}
for k, v in CLIS.items():
  CLI_COORDS[k] = (v._Screen__x + v._Screen__w/2, v._Screen__y + v._Screen__h/2)
BRK_COORDS = {}
for k, v in BRKS.items():
  BRK_COORDS[k] = (v._Screen__x + v._Screen__w/2, v._Screen__y + v._Screen__h/2)

ROOT = {
  "C": CLI_COORDS,
  "B": BRK_COORDS
  }

def lookup(path):
  m = ROOT
  for x in path.split(":"):
    m = m[x]
  return m

for i in range(250):
  window.redraw()

fade = 100
steps = 300
balls = []

for line in open("sequence"):
  if line.strip() == "--":
    balls.reverse()
    for b, _, _ in balls:
      window.add(b, 0, 0, 1.0, 1.0)
    balls.reverse()

    for b, start, stop in balls:
      cls, vnd = start.split(":")
      if cls == "B":
        b.vendor = BRKS[vnd].dequeue()
      else:
        for i in range(fade + 1):
          b.alpha = i/float(fade)
          window.redraw()

    for i in range(steps+1):
      for b, start, stop in balls:
        x1, y1 = lookup(start)
        x2, y2 = lookup(stop)
        dx = x2 - x1
        dy = y2 - y1
        b.x = x1 + dx*float(i)/steps
        b.y = y1 + dy*float(i)/steps
        b.angle = 2*pi*float(i)/steps
      window.redraw()

    for b, start, stop in balls:
      cls, vnd = stop.split(":")
      if cls == "B":
        BRKS[vnd].enqueue(b.vendor)
        window.redraw()
      else:
        for i in range(fade + 1):
          b.alpha = 1.0 - i/float(fade)
          window.redraw()
      window.remove(b)
    balls = []
    window.redraw()
  elif line.strip().startswith("+"):
    time.sleep(float(line.strip()[1:]))
  else:
    start, stop = line.split()
    cls, vnd = start.split(":")
    b = Ball(BRKS[vnd].vendor)
    b.x, b.y = lookup(start)
    if cls == "C":
      b.alpha = 0.0
    balls.append((b, start, stop))

time.sleep(1)
