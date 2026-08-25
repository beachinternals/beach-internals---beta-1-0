from ._anvil_designer import master_player_mgrTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
from anvil.tables import app_tables
from ..btd_form_helpers import parse_lgy, format_lgy, get_league_items


class master_player_mgr(master_player_mgrTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    user = anvil.users.get_user()
    if not user:
      alert("Not Currently Logged in")
      open_form('Homepage.Landing_form')
      return
    if not user['team']:
      alert('Please Contact Beach Internals to be Assigned to a Team')
      open_form('Homepage.Contact')
      return

    self.user = user
    self.user_team = user['team']

    # id -> item dict, for players checked for a merge
    self.merge_selected = {}
    # id of the player currently loaded in the edit panel, or None
    self.editing_id = None
    self.editing_item = None

    self.player_list_panel.set_event_handler('x-edit-player', self.player_list_panel_x_edit_player)
    self.player_list_panel.set_event_handler('x-toggle-merge', self.player_list_panel_x_toggle_merge)

    self.init_form()

  def init_form(self):
    self.league_dropdown.items = get_league_items(self.user_team)
    default_lgy = format_lgy(self.user['def_league'], self.user['def_gender'], self.user['def_year'])
    if default_lgy in self.league_dropdown.items:
      self.league_dropdown.selected_value = default_lgy
    elif self.league_dropdown.items:
      self.league_dropdown.selected_value = self.league_dropdown.items[0]

    self.reset_to_add_mode()

    if self.league_dropdown.selected_value:
      self.load_players()
    else:
      self.player_count_label.text = "No leagues found for your team"

  # ==========================================================================
  #  LOAD / SELECT
  # ==========================================================================

  def current_lgy(self):
    return parse_lgy(self.league_dropdown.selected_value)

  def load_players(self):
    league, gender, year = self.current_lgy()
    self.merge_selected = {}
    self.merge_selected_label.text = ""
    self.merge_selected_button.enabled = False

    players = anvil.server.call('get_master_players', league, gender, year)
    self.player_list_panel.items = players
    self.player_count_label.text = f"{len(players)} player(s)"

  def league_dropdown_change(self, **event_args):
    self.reset_to_add_mode()
    self.load_players()

  def player_list_panel_x_edit_player(self, item, **event_args):
    self.enter_edit_mode(item)

  def player_list_panel_x_toggle_merge(self, item, checked, **event_args):
    if checked:
      self.merge_selected[item['id']] = item
    else:
      self.merge_selected.pop(item['id'], None)

    count = len(self.merge_selected)
    self.merge_selected_label.text = f"{count} selected" if count else ""
    self.merge_selected_button.enabled = count >= 2

  def merge_selected_button_click(self, **event_args):
    if len(self.merge_selected) < 2:
      alert("Select at least two players to merge.")
      return
    self.enter_merge_mode()

  # ==========================================================================
  #  MODE SWITCHING
  # ==========================================================================

  def reset_to_add_mode(self, **event_args):
    self.mode = 'add'
    self.editing_id = None
    self.editing_item = None

    self.mode_label.text = "Add New Player"
    self.merge_summary_label.visible = False
    self.team_box.text = ""
    self.number_box.text = ""
    self.shortname_box.text = ""
    self.fullname_box.text = ""
    self.delete_button.visible = False
    self.primary_button.text = "Add Player"

  def enter_edit_mode(self, item):
    self.mode = 'edit'
    self.editing_id = item['id']
    self.editing_item = item

    self.mode_label.text = "Edit Player"
    self.merge_summary_label.visible = False
    self.team_box.text = item['team']
    self.number_box.text = item['number']
    self.shortname_box.text = item['shortname']
    self.fullname_box.text = item['fullname']
    self.delete_button.visible = True
    self.primary_button.text = "Save Changes"

  def enter_merge_mode(self):
    self.mode = 'merge'
    self.editing_id = None
    self.editing_item = None

    selected = list(self.merge_selected.values())
    self.mode_label.text = f"Merge {len(selected)} Players"
    lines = "\n".join(f"  • {p['team']} {p['number']} {p['shortname']}" for p in selected)
    self.merge_summary_label.text = f"Merging these players:\n{lines}"
    self.merge_summary_label.visible = True

    first = selected[0]
    self.team_box.text = first['team']
    self.number_box.text = first['number']
    self.shortname_box.text = first['shortname']
    self.fullname_box.text = first['fullname']
    self.delete_button.visible = False
    self.primary_button.text = "Merge Players"

  def cancel_button_click(self, **event_args):
    self.reset_to_add_mode()

  # ==========================================================================
  #  NORMALIZATION PREVIEW (mirrors server_code.master_player_mgr normalization,
  #  used only to detect an identity change before showing the confirm dialog)
  # ==========================================================================

  def _normalized_identity(self, team, number, shortname):
    team = (team or '').strip().upper()
    number = (number or '').strip()
    if len(number) == 1:
      number = '0' + number
    shortname = (shortname or '').strip()
    return team, number, shortname

  # ==========================================================================
  #  SAVE
  # ==========================================================================

  def primary_button_click(self, **event_args):
    if not self.team_box.text or not self.number_box.text or not self.shortname_box.text:
      alert("Team, number, and short name are all required.")
      return

    if self.mode == 'add':
      self.do_add()
    elif self.mode == 'edit':
      self.do_edit()
    elif self.mode == 'merge':
      self.do_merge()

  def do_add(self):
    league, gender, year = self.current_lgy()
    result = anvil.server.call(
      'add_master_player', league, gender, year,
      self.team_box.text, self.number_box.text, self.shortname_box.text, self.fullname_box.text
    )
    if not result['success']:
      alert(result['message'])
      return

    Notification(result['message'], style='success', timeout=3).show()
    self.reset_to_add_mode()
    self.load_players()

  def do_edit(self):
    editing_item = self.editing_item
    if editing_item is None:
      return
    new_identity = self._normalized_identity(self.team_box.text, self.number_box.text, self.shortname_box.text)
    old_identity = (editing_item['team'], editing_item['number'], editing_item['shortname'])

    if new_identity != old_identity:
      old_composed = " ".join(old_identity)
      new_composed = " ".join(new_identity)
      if not confirm(
        f'Change "{old_composed}" to "{new_composed}"?\n\n'
        f'Any Ball Time files using the old name will be updated to the new name '
        f'and queued for reprocessing tonight.'
      ):
        return

    result = anvil.server.call(
      'update_master_player', self.editing_id,
      self.team_box.text, self.number_box.text, self.shortname_box.text, self.fullname_box.text
    )
    if not result['success']:
      alert(result['message'])
      return

    Notification(result['message'], style='success', timeout=4).show()
    self.reset_to_add_mode()
    self.load_players()

  def do_merge(self):
    selected = list(self.merge_selected.values())
    identity = self._normalized_identity(self.team_box.text, self.number_box.text, self.shortname_box.text)
    new_composed = " ".join(identity)
    lines = "\n".join(f"  • {p['team']} {p['number']} {p['shortname']}" for p in selected)

    if not confirm(
      f'Merge these {len(selected)} players into "{new_composed}"?\n\n{lines}\n\n'
      f'Any Ball Time files referencing the old names will be updated to "{new_composed}" '
      f'and queued for reprocessing tonight. The other player record(s) will be deleted. '
      f'This cannot be undone.'
    ):
      return

    league, gender, year = self.current_lgy()
    result = anvil.server.call(
      'merge_master_players', league, gender, year, list(self.merge_selected.keys()),
      self.team_box.text, self.number_box.text, self.shortname_box.text, self.fullname_box.text
    )
    if not result['success']:
      alert(result['message'])
      return

    Notification(result['message'], style='success', timeout=4).show()
    self.reset_to_add_mode()
    self.load_players()

  # ==========================================================================
  #  DELETE
  # ==========================================================================

  def delete_button_click(self, **event_args):
    editing_item = self.editing_item
    if not self.editing_id or editing_item is None:
      return

    composed = f"{editing_item['team']} {editing_item['number']} {editing_item['shortname']}"
    if not confirm(f'Delete player "{composed}"?\n\nThis cannot be undone.'):
      return

    result = anvil.server.call('delete_master_player', self.editing_id)
    if not result['success']:
      alert(result['message'])
      return

    Notification(result['message'], style='success', timeout=3).show()
    self.reset_to_add_mode()
    self.load_players()

  def close_button_click(self, **event_args):
    open_form('Homepage.DataMgr')
