from ._anvil_designer import rpt_mgr_row_templateTemplate
from anvil import *


class rpt_mgr_row_template(rpt_mgr_row_templateTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    if self.item:
      self.update_display()

  def update_display(self):
    self.description_label.text = self.item['report_description'] or ''
    self.rpt_type_label.text = self.item['rpt_type'] or ''
    self.dow_label.text = self.item['dow'] or ''
    self.status_label.text = 'Active' if self.item['active'] == 'Yes' else 'Inactive'

  def edit_link_click(self, **event_args):
    """Bubble the 'edit this report' request up to the parent form"""
    self.parent.raise_event('x-edit-report', item=self.item)
