from ._anvil_designer import ai_export_mgrTemplate
from anvil import *
import anvil.server
import anvil.users

from ..btd_form_helpers import format_lgy, parse_lgy, get_league_items

# Front end for the ai_export_mgr table (Homepage.DataMgr.ai_export_mgr).
# Reached directly from the Homepage left nav ("AI Export"). Every list/add/
# update/delete call is routed through server_code/ai_export_mgr_admin.py,
# which scopes everything to the logged-in user's own team.
#
# export_type, user_email, de_identified, ai_optimized, and output_location
# are not exposed here -- they're always set to fixed values server-side.
#
# player_select / dataset_select are anvil_extras.MultiSelectDropDown --
# their own "selected" property is the source of truth, keyed by each
# option's value (a master_player id string for players, the full dataset
# row for datasets), so there's no separate selection-tracking dict here.

DOW_CHOICES = ['Everyday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']


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

    self.export_list_panel.set_event_handler('x-edit-export', self.export_list_panel_x_edit_export)

    self.init_form()

  def init_form(self):
    self.team_label.text = self.user['team']
    if self.user['team'] == 'INTERNALS':
      self.list_title.text = "All Teams' Exports"
    self.dow_dropdown.items = DOW_CHOICES

    self.lgy_dropdown.items = get_league_items(self.user['team'])
    default_lgy = format_lgy(self.user['def_league'], self.user['def_gender'], self.user['def_year'])
    if default_lgy in self.lgy_dropdown.items:
      self.lgy_dropdown.selected_value = default_lgy
    elif self.lgy_dropdown.items:
      self.lgy_dropdown.selected_value = self.lgy_dropdown.items[0]

    self.refresh_player_choices()

    datasets = anvil.server.call('get_export_datasets')
    self.dataset_select.items = [(r['dataset_name'] or r['dataset_type'] or 'Unnamed dataset', r) for r in datasets]

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

  def lgy_dropdown_change(self, **event_args):
    self.refresh_player_choices()

  def refresh_player_choices(self):
    league, gender, year = parse_lgy(self.lgy_dropdown.selected_value)
    players = anvil.server.call('get_export_players', league, gender, year) if league else []
    self.player_select.items = [
      (f"{p['team']} {p['number']} {p['shortname']}", p['id']) for p in players
    ]

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
    self.player_select.selected = []
    self.dataset_select.selected = []
    self.delete_button.visible = False
    self.primary_button.text = "Add Export"

  def enter_edit_mode(self, item):
    self.mode = 'edit'
    self.editing_id = item.get_id()

    self.mode_label.text = "Edit Export"
    self.note_box.text = item['Note'] or ''
    self.dow_dropdown.selected_value = item['dow']
    self.enabled_checkbox.checked = not item['disabled']

    self.lgy_dropdown.selected_value = self._lgy_for_item(item)
    self.refresh_player_choices()

    self.player_select.selected = [r.get_id() for r in (item['player_filter'] or [])]
    self.dataset_select.selected = list(item['datasets_included'] or [])

    self.delete_button.visible = True
    self.primary_button.text = "Save Changes"

  def _lgy_for_item(self, item):
    """
    Best-guess lgy dropdown value for an existing export row. The row itself
    only stores 'league' -- gender/year aren't columns on ai_export_mgr, so we
    derive them from whichever player is linked (master_player rows carry
    their own league/gender/year), same as the background generator does.
    Falls back to matching on league alone, then to the first available lgy.
    """
    players = item['player_filter']
    if players:
      p = players[0]
      lgy = format_lgy(p['league'], p['gender'], p['year'])
      if lgy in self.lgy_dropdown.items:
        return lgy

    league = item['league']
    for option in self.lgy_dropdown.items:
      if parse_lgy(option)[0] == league:
        return option

    return self.lgy_dropdown.items[0] if self.lgy_dropdown.items else None

  def cancel_button_click(self, **event_args):
    self.reset_to_add_mode()

  # ==========================================================================
  #  SAVE / DELETE
  # ==========================================================================

  def primary_button_click(self, **event_args):
    if not self.lgy_dropdown.selected_value:
      alert("Select a league / gender / year.")
      return
    if not (self.note_box.text or '').strip():
      alert("Please enter a title for this export.")
      return
    if not self.dow_dropdown.selected_value:
      alert("Please select a day of week.")
      return
    if not self.dataset_select.selected:
      alert("Select at least one dataset to include.")
      return

    if self.mode == 'add':
      self.do_add()
    else:
      self.do_edit()

  def do_add(self):
    league, _gender, _year = parse_lgy(self.lgy_dropdown.selected_value)
    result = anvil.server.call(
      'add_ai_export',
      league,
      self.note_box.text,
      self.dow_dropdown.selected_value,
      self.player_select.selected,
      self.dataset_select.selected,
      not self.enabled_checkbox.checked,
    )
    if not result['success']:
      alert(result['message'])
      return

    Notification(result['message'], style='success', timeout=3).show()
    self.reset_to_add_mode()
    self.refresh_export_list()

  def do_edit(self):
    league, _gender, _year = parse_lgy(self.lgy_dropdown.selected_value)
    result = anvil.server.call(
      'update_ai_export',
      self.editing_id,
      league,
      self.note_box.text,
      self.dow_dropdown.selected_value,
      self.player_select.selected,
      self.dataset_select.selected,
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
