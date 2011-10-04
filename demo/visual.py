import sys, time
from math import pi
from window import Window
from cairo import ImageSurface, LINE_CAP_ROUND

img = ImageSurface.create_from_png("button.png")

class Demo:

  def __init__(self):
    self.x = 0.0
    self.y = 0.0
    self.angle = 0.0

  def draw(self, cr, w, h):
    cr.translate(self.x, self.y)
    cr.translate(0.05, 0.0)
    cr.rotate(self.angle)
    cr.translate(-0.05, 0.0)
    cr.translate(0, 0.05)
    cr.scale(0.1/img.get_width(), -0.1/img.get_height())
    cr.set_source_surface(img)
    cr.paint()

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

class Broker:

  def __init__(self, vendor):
    self.vendor = vendor

  def draw(self, cr, w, h):
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
    xb, yb, w, h, xa, ya = cr.text_extents(self.vendor)
    cr.scale(0.8/w, -1.0)
    cr.show_text(self.vendor)
    cr.stroke()

class Client(Broker):
  pass

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

demo = Demo()

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
window.add(Broker("Red Hat"), bpad, 0.2, 0.1, 0.1)
window.add(Broker("VMWare"), 0.1 + 2*bpad, 0.2, 0.1, 0.1)
window.add(Broker("Microsoft"), 0.2 + 3*bpad, 0.2, 0.1, 0.1)
window.add(Broker("Inetco"), 0.3 + 4*bpad, 0.2, 0.1, 0.1)
window.add(Broker("Storm MQ"), 0.4 + 5*bpad, 0.2, 0.1, 0.1)
window.add(Broker("Apache Qpid"), 0.5 + 6*bpad, 0.2, 0.1, 0.1)
window.add(Broker("Swift MQ"), 0.6 + 7*bpad, 0.2, 0.1, 0.1)
window.add(demo, 0, 0, 1.0, 1.0)

first = True

for line in open("path"):
  x, y, steps = [float(n) for n in line.split()]
  steps = int(steps)
  if first:
    demo.x = x
    demo.y = y
    for i in range(steps):
      window.redraw()
    first = False
  else:
    xi = demo.x
    yi = demo.y
    dx = x - xi
    dy = y - yi
    for i in range(steps):
      demo.x = xi + dx*i/steps
      demo.y = yi + dy*i/steps
      demo.angle = 2*pi*i/steps
      window.redraw()

time.sleep(1)
