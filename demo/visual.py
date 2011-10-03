import sys, time
from math import pi
from window import Window
from cairo import ImageSurface

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

class Broker:

  def __init__(self, vendor):
    self.vendor = vendor

  def draw(self, cr, w, h):
    cr.set_source_rgb(0, 0, 0)
    cr.set_line_width(0.05)
    cr.move_to(0.0, 0.1)
    cr.arc(0.1, 0.1, 0.1, -pi, -pi/2.0)
    cr.line_to(0.9, 0.0)
    cr.arc(0.9, 0.1, 0.1, -pi/2.0, 0)
    cr.line_to(1.0, 0.9)
    cr.arc(0.9, 0.9, 0.1, 0.0, pi/2.0)
    cr.line_to(0.1, 1.0)
    cr.arc(0.1, 0.9, 0.1, pi/2.0, pi)
    cr.close_path()
    cr.stroke_preserve()
    cr.set_source_rgb(0.9, 0.9, 1.0)
    cr.fill()
    cr.set_source_rgb(0.75, 0, 0)
    cr.move_to(0.1, 0.4)
    cr.set_font_size(0.2)
    xb, yb, w, h, xa, ya = cr.text_extents(self.vendor)
    cr.scale(0.8/w, -1.0)
    cr.show_text(self.vendor)

demo = Demo()

window = Window()
window.add(Broker("Red Hat"), 0.1, 0.2, 0.1, 0.1)
window.add(Broker("VMWare"), 0.3, 0.2, 0.1, 0.1)
window.add(Broker("Microsoft"), 0.5, 0.2, 0.1, 0.1)
window.add(Broker("Inetco"), 0.7, 0.2, 0.1, 0.1)
window.add(Broker("Storm MQ"), 0.1, 0.8, 0.1, 0.1)
window.add(Broker("Apache Qpid"), 0.3, 0.8, 0.1, 0.1)
window.add(Broker("Swift MQ"), 0.5, 0.8, 0.1, 0.1)
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
