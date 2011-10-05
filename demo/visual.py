import sys, time
from math import pi
from window import Window
from cairo import ImageSurface, LINE_CAP_ROUND

img = ImageSurface.create_from_png("button96.png")

class Demo:

  def __init__(self):
    self.x = 0.0
    self.y = 0.0
    self.angle = 0.0
    self.alpha = 1.0

  def draw(self, cr, w, h):
    cr.translate(self.x, self.y)
    cr.translate(0.05, 0.0)
    cr.rotate(self.angle)
    cr.translate(-0.05, 0.0)
    cr.translate(0, 0.05)
    cr.scale(0.1/img.get_width(), -0.1/img.get_height())
    cr.set_source_surface(img)
    cr.paint_with_alpha(self.alpha)

def box(cr):
  cr.move_to(0.0, 0.1)
  cr.arc(0.1, 0.1, 0.1, -pi, -pi/2.0)
  cr.line_to(0.9, 0.0)
  cr.arc(0.9, 0.1, 0.1, -pi/2.0, 0)
  cr.line_to(1.0, 0.9)
  cr.arc(0.9, 0.9, 0.1, 0.0, pi/2.0)
  cr.line_to(0.1, 1.0)
  cr.arc(0.1, 0.9, 0.1, pi/2.0, pi)
  cr.close_path()

def component(cr, text):
    cr.set_source_rgb(0, 0, 0)
    cr.set_line_width(0.05)
    cr.translate(-0.03, 0.03)
    box(cr)
    cr.set_source_rgb(0.75, 0.75, 0.75)
    cr.stroke()
    cr.translate(0.03, -0.03)
    box(cr)
    cr.set_source_rgb(0, 0, 0)
    cr.stroke_preserve()
    cr.set_source_rgb(0.9, 0.9, 1.0)
    cr.fill()
    cr.set_source_rgb(0.75, 0, 0)
    cr.move_to(0.1, 0.4)
    cr.set_font_size(0.2)
    xb, yb, w, h, xa, ya = cr.text_extents(text)
    cr.scale(0.8/w, -1.0)
    cr.show_text(text)
    cr.stroke()

class Broker:

  def __init__(self, vendor):
    self.vendor = vendor
    self.depth = 0

  def draw(self, cr, w, h):
    cr.save()
    component(cr, self.vendor)
    cr.restore()
    cr.translate(0.25, 0.6)
    for i in range(self.depth):
      cr.save()
      cr.translate(0, 0.1*i)
      cr.scale(0.5/img.get_width(), -0.5/img.get_height())
      cr.set_source_surface(img)
      cr.paint()
      cr.restore()
#    cr.show_text(str(self.depth))

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

CLI_COORDS = {
  "RH": (0.0571428571429, 0.7),
  "MS": (0.214285714286,  0.7),
  "IN": (0.371428571429,  0.7),
  "ST": (0.528571428571,  0.7),
  "AP": (0.685714285714,  0.7),
  "SW": (0.842857142857,  0.7)
  }

BRK_COORDS = {
  "RH": (0.0375, 0.3),
  "VM": (0.175, 0.3),
  "MS": (0.45, 0.3),
  "IN": (0.5875, 0.3),
  "ST": (0.3125, 0.3),
  "AP": (0.725, 0.3),
  "SW": (0.8625, 0.3)
  }

ROOT = {
  "C": CLI_COORDS,
  "B": BRK_COORDS
  }

BRKS = {
  "RH": Broker("Red Hat"),
  "VM": Broker("VMWare"),
  "MS": Broker("Microsoft"),
  "IN": Broker("Inetco"),
  "ST": Broker("Storm MQ"),
  "AP": Broker("Apache Qpid"),
  "SW": Broker("Swift MQ")
  }

window = Window()
cpad = 0.4/7
window.add(Text("Clients"), 0.5, 0.85, 0.7, 0.1)
window.add(Client("Red Hat"), cpad, 0.7, 0.1, 0.1)
window.add(Client("Microsoft"), 0.1 + 2*cpad, 0.7, 0.1, 0.1)
window.add(Client("Inetco"), 0.2 + 3*cpad, 0.7, 0.1, 0.1)
window.add(Client("Storm MQ"), 0.3 + 4*cpad, 0.7, 0.1, 0.1)
window.add(Client("Apache Qpid"), 0.4 + 5*cpad, 0.7, 0.1, 0.1)
window.add(Client("Swift MQ"), 0.5 + 6*cpad, 0.7, 0.1, 0.1)

window.add(Line(0.0375, 0.5, 1 - 0.0375, 0.5), 0, 0, 1, 1)

window.add(Text("Brokers"), 0.5, 0.05, 0.7, 0.1)
bpad = 0.3/8
window.add(BRKS["RH"], bpad, 0.2, 0.1, 0.1)
window.add(BRKS["VM"], 0.1 + 2*bpad, 0.2, 0.1, 0.1)
window.add(BRKS["MS"], 0.2 + 3*bpad, 0.2, 0.1, 0.1)
window.add(BRKS["IN"], 0.3 + 4*bpad, 0.2, 0.1, 0.1)
window.add(BRKS["ST"], 0.4 + 5*bpad, 0.2, 0.1, 0.1)
window.add(BRKS["AP"], 0.5 + 6*bpad, 0.2, 0.1, 0.1)
window.add(BRKS["SW"], 0.6 + 7*bpad, 0.2, 0.1, 0.1)

def lookup(path):
  m = ROOT
  for x in path.split(":"):
    m = m[x]
  return m


for i in range(250):
  window.redraw()

steps = 250
balls = []

for line in open("sequence"):
  if line.strip() == "--":
    for b, start, stop in balls:
      cls, brk = start.split(":")
      if cls == "B":
        BRKS[brk].depth -= 1
    for i in range(steps):
      for b, start, stop in balls:
        x1, y1 = lookup(start)
        x2, y2 = lookup(stop)
        dx = x2 - x1
        dy = y2 - y1
        b.x = x1 + dx*i/steps
        b.y = y1 + dy*i/steps
        b.angle = 2*pi*i/steps
      window.redraw()

    for b, start, stop in balls:
      cls, brk = stop.split(":")
      if cls == "B":
        BRKS[brk].depth += 1
        window.redraw()
      else:
        for i in range(50):
          b.alpha = 1.0 - i/50.0
          window.redraw()
      window.remove(b)
    balls = []
    window.redraw()
  elif line.strip().startswith("+"):
    time.sleep(float(line.strip()[1:]))
  else:
    start, stop = line.split()
    b = Demo()
    b.x, b.y = lookup(start)
    window.add(b, 0, 0, 1.0, 1.0)
    balls.append((b, start, stop))

time.sleep(3)
