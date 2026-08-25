from ._anvil_designer import ai_export_mgrTemplate
from anvil import *
import anvil.server
import anvil.users

# Front end for the ai_export_mgr table (Homepage.DataMgr.ai_export_mgr).
# Reached directly from the Homepage left nav ("AI Export"). Every list/add/
# update/delete call is routed through server_code/ai_export_mgr_admin.py,
# which scopes everything to the logged-in user's own team.
#
# export_type, user_email, de_identified, ai_optimized, and output_location
# are not exposed here -- they're always set to fixed values server-side.

DOW_CHOICES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']


class ai_export_mgr(ai_export_mgrTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    user = anvil.users.get_user(allow_remembered=True)
    if not user:
      alert("Not Currently Logged in")
      open_form('Homepage.Landing_form')
      return
    if not user['team']:
      alert('Please Contact Beach Internals to be Assigned to a Team')
      open_form('Homepage.Contact')
      return

    self.user = user
    self.mode = 'add'
    self.editing_id = None
    self.selected_players = {}
    self.player_found_count = 0
    self.all_datasets = []
    self.selected_datasets = {}

    self.export_list_panel.set_event_handler('x-edit-export', self.export_list_panel_x_edit_export)
    self.player_list_panel.set_event_handler('x-toggle-player', self.player_list_panel_x_toggle_player)
    self.dataset_list_panel.set_event_handler('x-toggle-dataset', self.dataset_list_panel_x_toggle_dataset)

    self.init_form()

  def init_form(self):
    self.team_label.text = self.user['team']
    self.dow_dropdown.items = DOW_CHOICES

    self.league_dropdown.items = anvil.server.call('get_export_league_choices')
    default_league = self.user['def_league']
    if default_league in self.league_dropdown.items:
      self.league_dropdown.selected_value = default_league
    elif self.league_dropdown.items:
      self.league_dropdown.selected_value = self.league_dropdown.items[0]

    self.all_datasets = anvil.server.call('get_export_datasets')
    self.refresh_dataset_panel()

    self.refresh_export_list()
    self.reset_to_add_mode()

  # ==========================================================================
  #  EXPORT LIST
  # ==========================================================================

  def refresh_export_list(self):
    exports = anvil.server.call('list_ai_exports')
    self.export_list_panel.items = exports
    self.export_count_label.text = f"{len(exports)} export(s)" if exports else "No exports yet"

  def export_list_panel_x_edit_export(self, item, **event_args):
    self.enter_edit_mode(item)

  # ==========================================================================
  #  PLAYER PICKER
  # ==========================================================================

  def league_dropdown_change(self, **event_args):
    self.selected_players = {}
    self.search_box.text = ""
    self.clear_player_results()

  def clear_player_results(self):
    """Empty the player list rather than loading a league's whole roster up front."""
    self.player_list_panel.items = []
    self.player_found_count = 0
    self.update_player_count_label()

  def search_button_click(self, **event_args):
    league = self.league_dropdown.selected_value
    if not league:
      alert("Select a league first.")
      return
    search_text = (self.search_box.text or '').strip()
    if not search_text:
      alert("Enter a team, number, or short name to search.")
      return

    matches = anvil.server.call('get_export_players', league, search_text)
    self.player_list_panel.items = [
      {**m, 'checked': m['id'] in self.selected_players} for m in matches
    ]
    self.player_found_count = len(matches)
    self.update_player_count_label()

  def update_player_count_label(self):
    selected = len(self.selected_players)
    if self.player_found_count:
      self.player_count_label.text = f"{self.player_found_count} found, {selected} selected"
    elif selected:
      self.player_count_label.text = f"{selected} player(s) selected"
    else:
      self.player_count_label.text = "Search for a player to add them to this export"

  def player_list_panel_x_toggle_player(self, item, checked, **event_args):
    if checked:
      self.selected_players[item['id']] = item
    else:
      self.selected_players.pop(item['id'], None)
    self.update_player_count_label()

  # ==========================================================================
  #  DATASET PICKER
  # ==========================================================================

  def refresh_dataset_panel(self):
    self.dataset_list_panel.items = [
      {'row': r, 'checked': r.get_id() in self.selected_datasets} for r in self.all_datasets
    ]
    self.update_dataset_count_label()

  def update_dataset_count_label(self):
    count = len(self.selected_datasets)
    self.dataset_count_label.text = f"{count} dataset(s) selected" if count else "Select at least one dataset"

  def dataset_list_panel_x_toggle_dataset(self, row, checked, **event_args):
    if checked:
      self.selected_datasets[row.get_id()] = row
    else:
      self.selected_datasets.pop(row.get_id(), None)
    self.update_dataset_count_label()

  # ==========================================================================
  #  MODE SWITCHING
  # ==========================================================================

  def reset_to_add_mode(self, **event_args):
    self.mode = 'add'
    self.editing_id = None

    self.mode_label.text = "Add New Export"
    self.note_box.text = ""
    self.dow_dropdown.selected_value = None
    self.enabled_checkbox.checked = True
    self.selected_players = {}
    self.selected_datasets = {}
    self.search_box.text = ""
    self.clear_player_results()
    self.refresh_dataset_panel()
    self.delete_button.visible = False
    self.primary_button.text = "Add Export"

  def enter_edit_mode(self, item):
    self.mode = 'edit'
    self.editing_id = item.get_id()

    self.mode_label.text = "Edit Export"
    self.note_box.text = item['Note'] or ''
    self.dow_dropdown.selected_value = item['dow']
    self.enabled_checkbox.checked = not item['disabled']

    league = item['league']
    if league in self.league_dropdown.items:
      self.league_dropdown.selected_value = league

    self.selected_players = {
      r.get_id(): {'id': r.get_id(), 'team': r['team'], 'number': r['number'], 'shortname': r['shortname']}
      for r in (item['player_filter'] or [])
    }
    self.selected_datasets = {r.get_id(): r for r in (item['datasets_included'] or [])}
    self.search_box.text = ""
    self.clear_player_results()
    self.refresh_dataset_panel()

    self.delete_button.visible = True
    self.primary_button.text = "Save Changes"

  def cancel_button_click(self, **event_args):
    self.reset_to_add_mode()

  # ==========================================================================
  #  SAVE / DELETE
  # ==========================================================================

  def primary_button_click(self, **event_args):
    if not self.league_dropdown.selected_value:
      alert("Select a league.")
      return
    if not (self.note_box.text or '').strip():
      alert("Please enter a title for this export.")
      return
    if not self.dow_dropdown.selected_value:
      alert("Please select a day of week.")
      return
    if not self.selected_datasets:
      alert("Select at least one dataset to include.")
      return

    if self.mode == 'add':
      self.do_add()
    else:
      self.do_edit()

  def do_add(self):
    result = anvil.server.call(
      'add_ai_export',
      self.league_dropdown.selected_value,
      self.note_box.text,
      self.dow_dropdown.selected_value,
      list(self.selected_players.keys()),
      list(self.selected_datasets.values()),
      not self.enabled_checkbox.checked,
    )
    if not result['success']:
      alert(result['message'])
      return

    Notification(result['message'], style='success', timeout=3).show()
    self.reset_to_add_mode()
    self.refresh_export_list()

  def do_edit(self):
    result = anvil.server.call(
      'update_ai_export',
      self.editing_id,
      self.league_dropdown.selected_value,
      self.note_box.text,
      self.dow_dropdown.selected_value,
      list(self.selected_players.keys()),
      list(self.selected_datasets.values()),
      not self.enabled_checkbox.checked,
    )
    if not result['success']:
      alert(result['message'])
      return

    Notification(result['message'], style='success', timeout=4).show()
    self.reset_to_add_mode()
    self.refresh_export_list()

  def delete_button_click(self, **event_args):
    if not self.editing_id:
      return
    note = self.note_box.text or 'this export'
    if not confirm(f'Delete "{note}"?\n\nThis cannot be undone.'):
      return

    result = anvil.server.call('delete_ai_export', self.editing_id)
    if not result['success']:
      alert(result['message'])
      return

    Notification(result['message'], style='success', timeout=3).show()
    self.reset_to_add_mode()
    self.refresh_export_list()

  def close_button_click(self, **event_args):
    open_form('Homepage.DataMgr')
