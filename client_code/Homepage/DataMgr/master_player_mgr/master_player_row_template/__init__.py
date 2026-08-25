from ._anvil_designer import master_player_row_templateTemplate
from anvil import *
import anvil.server
import anvil.tables as tables
from anvil.tables import app_tables


class master_player_row_template(master_player_row_templateTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    if self.item:
      self.update_display()

  def update_display(self):
    self.team_number_label.text = f"{self.item['team']} {self.item['number']}"
    self.shortname_label.text = self.item['shortname']
    self.fullname_label.text = self.item['fullname'] or ''

  def merge_checkbox_change(self, **event_args):
    """Bubble this row's merge selection state up to the parent form"""
    self.parent.raise_event('x-toggle-merge', item=self.item, checked=self.merge_checkbox.checked)

  def edit_link_click(self, **event_args):
    """Bubble the 'edit this player' request up to the parent form"""
    self.parent.raise_event('x-edit-player', item=self.item)
