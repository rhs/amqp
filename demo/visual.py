from window import Window
from cairo import ImageSurface

button = ImageSurface.create_from_png("/home/rhs/Downloads/button.png")

class Demo:

  def __init__(self):
    self.x = 0.0
    self.y = 0.0

  def draw(self, cr, w, h):
    cr.translate(self.x, self.y)
    self.x += 0.01
    self.y += 0.01
    cr.scale(1.0/button.get_width(), 1.0/button.get_height())
    cr.set_source_surface(button)
    cr.paint()
    print "hi"

window = Window()
window.add(Demo(), 0, 0, 1.0, 1.0)
window.run()
