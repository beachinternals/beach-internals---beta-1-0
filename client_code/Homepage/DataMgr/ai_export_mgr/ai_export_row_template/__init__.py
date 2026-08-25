from ._anvil_designer import ai_export_row_templateTemplate
from anvil import *


class ai_export_row_template(ai_export_row_templateTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    if self.item:
      self.update_display()

  def update_display(self):
    self.league_label.text = self.item['league'] or ''
    self.note_label.text = self.item['Note'] or ''
    self.dow_label.text = self.item['dow'] or ''
    self.status_label.text = 'Disabled' if self.item['disabled'] else 'Enabled'

  def edit_link_click(self, **event_args):
    """Bubble the 'edit this export' request up to the parent form"""
    self.parent.raise_event('x-edit-export', item=self.item)
