import sys, time
from math import pi
from window import Window
from cairo import ImageSurface, LINE_CAP_ROUND

if "unicorn" in sys.argv:
  img = ImageSurface.create_from_png("unicorn96.png")
  vendor_offset = 0.025
  vendor_color = (1, 1, 1)
elif "blue" in sys.argv:
  img = ImageSurface.create_from_png("bluebubble96.png")
  vendor_offset = 0
  vendor_color = (1, 1, 1)
else:
  img = ImageSurface.create_from_png("bubble96.png")
  vendor_offset = 0
  vendor_color = (0.5, 0.5, 0.5)

class Ball:

  def __init__(self, vendor):
    self.vendor = vendor
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
    cr.save()
    cr.scale(0.1/img.get_width(), -0.1/img.get_height())
    cr.set_source_surface(img)
    cr.paint_with_alpha(self.alpha)
    cr.restore()
    cr.set_source_rgba(*(vendor_color + (self.alpha,)))
    cr.translate(0.035, -0.06 + vendor_offset)
    cr.set_font_size(0.05)
    xb, yb, w, h, xa, ya = cr.text_extents(self.vendor)
    cr.scale(0.06/w, -1.0)
    cr.scale(0.5, 0.5)
    cr.show_text(self.vendor)
    cr.stroke()

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
    self.messages = []

  def enqueue(self, item):
    self.messages.append(item)

  def dequeue(self):
    return self.messages.pop(0)

  def draw(self, cr, w, h):
    cr.save()
    component(cr, self.vendor)
    cr.restore()
    cr.translate(0.25, 0.8)
    for i in range(len(self.messages)):
      cr.save()
      cr.translate(0, 0.1*(i - len(self.messages)/2.0))
      cr.scale(0.5/img.get_width(), -0.5/img.get_height())
      cr.set_source_surface(img)
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

CLI_COORDS = {
  "RH": (0.0571428571429, 0.75),
  "MS": (0.214285714286,  0.75),
  "IN": (0.371428571429,  0.75),
  "ST": (0.528571428571,  0.75),
  "AP": (0.685714285714,  0.75),
  "SW": (0.842857142857,  0.75)
  }

BRK_COORDS = {
  "RH": (0.0375, 0.25),
  "VM": (0.175, 0.25),
  "MS": (0.45, 0.25),
  "IN": (0.5875, 0.25),
  "ST": (0.3125, 0.25),
  "AP": (0.725, 0.25),
  "SW": (0.8625, 0.25)
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
      cls, vnd = start.split(":")
      if cls == "B":
        b.vendor = BRKS[vnd].dequeue()
      else:
        for i in range(51):
          b.alpha = i/50.0
          window.redraw()

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
      cls, vnd = stop.split(":")
      if cls == "B":
        BRKS[vnd].enqueue(start.split(":")[-1])
        window.redraw()
      else:
        for i in range(51):
          b.alpha = 1.0 - i/50.0
          window.redraw()
      window.remove(b)
    balls = []
    window.redraw()
  elif line.strip().startswith("+"):
    time.sleep(float(line.strip()[1:]))
  else:
    start, stop = line.split()
    cls, vnd = start.split(":")
    b = Ball(vnd)
    b.x, b.y = lookup(start)
    if cls == "C":
      b.alpha = 0.0
    window.add(b, 0, 0, 1.0, 1.0)
    balls.append((b, start, stop))

time.sleep(3)
