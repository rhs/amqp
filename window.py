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

import pygtk
pygtk.require('2.0')
import gtk, gobject, cairo
from threading import Thread

class Screen(gtk.DrawingArea):

  __gsignals__ = { "expose-event": "override" }

  def __init__(self):
    gtk.DrawingArea.__init__(self)
    self.widgets = []

  def add(self, widget, x, y, w, h):
    self.widgets.append((widget, x, y, w, h))

  def do_expose_event(self, event):
    cr = self.window.cairo_create()
    cr.rectangle(event.area.x, event.area.y,
                 event.area.width, event.area.height)
    cr.clip()
    self.draw(cr, *self.window.get_size())

  def draw(self, cr, width, height):
    cr.set_source_rgb(1.0, 1.0, 1.0)
    cr.rectangle(0, 0, width, height)
    cr.fill()

    cr.translate(0, height)
    cr.scale(width/1.0, -height/1.0)

    for widget, x, y, w, h in self.widgets:
      cr.save()
      cr.translate(x, y)
      cr.scale(w/1.0, h/1.0)
      widget.draw(cr, w, h)
      cr.restore()

class Window(Thread):

  def __init__(self):
    Thread.__init__(self)
    self.window = gtk.Window()
    self.window.connect("delete-event", gtk.main_quit)
    self.screen = Screen()
    self.screen.show()
    self.window.add(self.screen)
    self.window.present()

  def add(self, widget, x, y, w, h):
    self.screen.add(widget, x, y, w, h)

  def redraw(self):
    self.window.queue_draw()
    return True

  def run(self):
    gobject.timeout_add(40, self.redraw)
    gtk.main()
