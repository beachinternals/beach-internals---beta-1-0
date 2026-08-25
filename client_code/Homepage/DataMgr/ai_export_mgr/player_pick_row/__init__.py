from ._anvil_designer import player_pick_rowTemplate
from anvil import *

# self.item is {'id': ..., 'team': ..., 'number': ..., 'shortname': ..., 'checked': bool}
# -- a plain dict from get_export_players(), not a linked master_player row. See
# ai_export_mgr's search_button_click(), which rebuilds this shape from the current
# search results and self.selected_players every time a search runs.


class player_pick_row(player_pick_rowTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    if self.item:
      self.team_number_label.text = f"{self.item['team']} {self.item['number']}"
      self.shortname_label.text = self.item['shortname']
      self.select_checkbox.checked = self.item['checked']

  def select_checkbox_change(self, **event_args):
    """Bubble this row's selection state up to the parent form"""
    self.parent.raise_event('x-toggle-player', item=self.item, checked=self.select_checkbox.checked)
