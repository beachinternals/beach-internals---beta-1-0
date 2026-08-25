from ._anvil_designer import dataset_pick_rowTemplate
from anvil import *

# self.item is {'row': <ai_export_dataset_list row>, 'checked': bool} -- see
# ai_export_mgr's refresh_dataset_panel().


class dataset_pick_row(dataset_pick_rowTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    if self.item:
      row = self.item['row']
      self.dataset_name_label.text = row['dataset_name'] or ''
      self.dataset_type_label.text = row['dataset_type'] or ''
      self.select_checkbox.checked = self.item['checked']

  def select_checkbox_change(self, **event_args):
    """Bubble this row's selection state up to the parent form"""
    self.parent.raise_event('x-toggle-dataset', row=self.item['row'], checked=self.select_checkbox.checked)
