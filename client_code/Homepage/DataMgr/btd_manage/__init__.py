from ._anvil_designer import btd_manageTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
from anvil.tables import app_tables
from ..btd_form_helpers import (
  parse_lgy, format_lgy, get_league_items,
  get_comp_l1_items, get_comp_l2_items, get_comp_l3_items,
  get_venue_items, get_ppr_player_list, unpack_btd_statistics
)


class btd_manage(btd_manageTemplate):
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

    # Currently selected file
    self.selected_file_id = None
    self.selected_file_details = None
    self.file_replaced = False
    self.pending_stats = None

    # The RepeatingPanel item template bubbles its selection event onto
    # itself via self.parent.raise_event(...); listen for it here.
    self.file_list_panel.set_event_handler('x-select-file', self.file_list_panel_x_select_file)

    self.init_form()

  def init_form(self):
    """Initialize the filter panel and load the file list"""

    self.league_dropdown.items = get_league_items(self.user_team)
    default_lgy = format_lgy(self.user['def_league'], self.user['def_gender'], self.user['def_year'])
    if default_lgy in self.league_dropdown.items:
      self.league_dropdown.selected_value = default_lgy
    elif self.league_dropdown.items:
      self.league_dropdown.selected_value = self.league_dropdown.items[0]

    self.sort_dropdown.items = [
      ('Date (Match)', 'date'),
      ('Date (Uploaded)', 'upload_date'),
      ('Completeness Score', 'completeness'),
      ('% with X,Y', 'per_xy'),
      ('Errors', 'errors'),
    ]
    self.sort_dropdown.selected_value = 'date'

    # Don't load anything until the user actually searches -- an unfiltered
    # load on every page open is slow.
    self.file_count_label.text = "Set your search criteria and click Apply Filters"

  def build_filters(self):
    filters = {}

    league, gender, year = parse_lgy(self.league_dropdown.selected_value)
    if league:
      filters['league'] = league
      filters['gender'] = gender
      filters['year'] = year

    if self.date_from_picker.date:
      filters['date_from'] = self.date_from_picker.date
    if self.date_to_picker.date:
      filters['date_to'] = self.date_to_picker.date

    if self.min_completeness_box.text:
      try:
        filters['min_completeness'] = float(self.min_completeness_box.text)
      except ValueError:
        pass

    if self.has_errors_checkbox.checked:
      filters['has_errors'] = True

    if self.filename_search_box.text:
      filters['filename_search'] = self.filename_search_box.text

    filters['sort_by'] = self.sort_dropdown.selected_value
    filters['sort_desc'] = True

    return filters

  def load_files(self):
    """Load the file list for the current filters"""
    files = anvil.server.call('get_btd_files_filtered', self.build_filters())
    self.file_list_panel.items = files
    self.file_count_label.text = f"{len(files)} files found"

  def apply_filters_button_click(self, **event_args):
    self.load_files()

  def clear_filters_button_click(self, **event_args):
    self.filename_search_box.text = ""
    self.date_from_picker.date = None
    self.date_to_picker.date = None
    self.min_completeness_box.text = ""
    self.has_errors_checkbox.checked = False

  def sort_dropdown_change(self, **event_args):
    self.load_files()

  # ==========================================================================
  #  SELECT A FILE / POPULATE THE EDIT PANEL
  # ==========================================================================

  def file_list_panel_x_select_file(self, item, **event_args):
    """Raised by a row's 'View / Correct' link"""
    self.select_file(item['id'])

  def select_file(self, file_id):
    details = anvil.server.call('get_btd_file_details', file_id)

    self.selected_file_id = file_id
    self.selected_file_details = details
    self.file_replaced = False
    self.pending_stats = None

    self.populate_edit_panel(details)

    self.select_file_label.visible = False
    self.edit_card.visible = True

  def populate_edit_panel(self, details):
    league, gender, year = details['league'], details['gender'], details['year']

    self.file_name_label.text = details['filename']
    self.context_label.text = f"{league} | {gender} | {year}  —  {details['team']}"

    # --- Metadata ---
    self.comp_l1_dropdown.items = get_comp_l1_items(league)
    self.comp_l1_dropdown.selected_value = self._match_row(self.comp_l1_dropdown.items, 'comp_l1', details['comp_l1'])

    self.comp_l2_dropdown.items = get_comp_l2_items(league, details['comp_l1'])
    self.comp_l2_dropdown.selected_value = self._match_row(self.comp_l2_dropdown.items, 'comp_l2', details['comp_l2'])

    self.comp_l3_dropdown.items = get_comp_l3_items(league)
    self.comp_l3_dropdown.selected_value = self._match_row(self.comp_l3_dropdown.items, 'comp_l3', details['comp_l3'])

    self.edit_date_picker.date = details['date']

    self.edit_venue_dropdown.items = get_venue_items(league)
    self.edit_venue_dropdown.selected_value = self._match_row(self.edit_venue_dropdown.items, 'venue_name', details['venue_name'])

    self.edit_match_time_dropdown.selected_value = details['match_time']

    self.edit_private_radio.selected = bool(details['private'])
    self.edit_scouting_radio.selected = not details['private']

    # --- Stats / errors ---
    self.stats_label.text = (
      f"Points: {details['points'] or 0}    "
      f"Completeness: {self._pct(details['completeness_score'])}    "
      f"% w/ Players: {self._pct(details['per_players'])}    "
      f"% w/ Serve Player: {self._pct(details['per_srv_players'])}    "
      f"% w/ X,Y: {self._pct(details['per_xy'])}"
    )

    if details['no_errors']:
      self.errors_label.visible = True
      self.errors_label.text = f"⚠ {details['no_errors']} error(s) found on the last processing run"
    else:
      self.errors_label.visible = False

    if details['ppr_file_date']:
      self.last_processed_label.text = f"Last processed: {details['ppr_file_date'].strftime('%b %d, %Y %I:%M %p')}"
    else:
      self.last_processed_label.text = "Not yet processed"

    # --- Replace-file controls ---
    self.replace_file_loader.clear()
    self.replace_status_label.visible = False

    # --- Player mapping ---
    btd_players = [details['player1'], details['player2'], details['player3'], details['player4']]
    for dd in (self.btd_playera1_dropdown, self.btd_playera2_dropdown, self.btd_playerb1_dropdown, self.btd_playerb2_dropdown):
      dd.items = btd_players
    self.btd_playera1_dropdown.selected_value = details['player1']
    self.btd_playera2_dropdown.selected_value = details['player2']
    self.btd_playerb1_dropdown.selected_value = details['player3']
    self.btd_playerb2_dropdown.selected_value = details['player4']

    ppr_players = get_ppr_player_list(league, gender, year)
    for dd in (self.ppr_playera1_dropdown, self.ppr_playera2_dropdown, self.ppr_playerb1_dropdown, self.ppr_playerb2_dropdown):
      dd.items = ppr_players
    self.ppr_playera1_dropdown.selected_value = details['ppr_playera1']
    self.ppr_playera2_dropdown.selected_value = details['ppr_playera2']
    self.ppr_playerb1_dropdown.selected_value = details['ppr_playerb1']
    self.ppr_playerb2_dropdown.selected_value = details['ppr_playerb2']

  def _pct(self, value):
    return f"{value:.1f}%" if value is not None else "N/A"

  def _match_row(self, dropdown_items, key, value):
    """Find the (display, row) item whose row[key] == value, for pre-selecting a DropDown."""
    for display, row in dropdown_items:
      if row[key] == value:
        return row
    return None

  def comp_l1_dropdown_change(self, **event_args):
    league = self.selected_file_details['league']
    self.comp_l2_dropdown.items = get_comp_l2_items(league, self.comp_l1_dropdown.selected_value['comp_l1'])

  def privacy_help_link_click(self, **event_args):
    alert("This defines how the data will be used:\n\nPrivate - Only available to the team, used for Player and Pair reports.\n\nScouting - Used for scouting, available to all teams on the Internals network.")

  # ==========================================================================
  #  REPLACE THE ACTIONS FILE
  # ==========================================================================

  def replace_file_loader_change(self, file, **event_args):
    with Notification("Checking replacement file...", style="info", timeout=None):
      result = anvil.server.call('check_replacement_btd_file', self.selected_file_id, file)

    if not result['valid']:
      self.replace_status_label.visible = True
      self.replace_status_label.foreground = '#dc3545'
      self.replace_status_label.text = f"⚠ {result['message']}"
      alert(f"BTD File Error - Please fix in Balltime and re-export:\n\n{result['message']}")
      return

    stats = unpack_btd_statistics(result['statistics'], result['cleaned_csv'], file.name)
    self.pending_stats = stats
    self.file_replaced = True

    for dd in (self.btd_playera1_dropdown, self.btd_playera2_dropdown, self.btd_playerb1_dropdown, self.btd_playerb2_dropdown):
      dd.items = stats['players']
    self.btd_playera1_dropdown.selected_value = stats['players'][0]
    self.btd_playera2_dropdown.selected_value = stats['players'][1]
    self.btd_playerb1_dropdown.selected_value = stats['players'][2]
    self.btd_playerb2_dropdown.selected_value = stats['players'][3]

    self.replace_status_label.visible = True
    self.replace_status_label.foreground = '#28a745'
    self.replace_status_label.text = (
      f"New file ready: {stats['points']} points, {stats['completeness_score']:.1f}% complete. "
      f"Review the player mapping below, then Save Changes."
    )
    Notification("File checked OK.", style="success", timeout=3).show()

  # ==========================================================================
  #  SAVE / DELETE
  # ==========================================================================

  def save_button_click(self, **event_args):
    if not self.edit_date_picker.date:
      alert("Please enter a valid date for this Match")
      return
    if not self.comp_l1_dropdown.selected_value or not self.comp_l2_dropdown.selected_value or not self.comp_l3_dropdown.selected_value:
      alert("Please select a competition level 1, 2, and 3 for this match.")
      return
    if not self.edit_venue_dropdown.selected_value:
      alert("Please select a venue for this match.")
      return

    fields = {
      'comp_l1': self.comp_l1_dropdown.selected_value['comp_l1'],
      'comp_l2': self.comp_l2_dropdown.selected_value['comp_l2'],
      'comp_l3': self.comp_l3_dropdown.selected_value['comp_l3'],
      'date': self.edit_date_picker.date,
      'private': self.edit_private_radio.selected,
      'venue_name': self.edit_venue_dropdown.selected_value['venue_name'],
      'venue_id': self.edit_venue_dropdown.selected_value['venue_id'],
      'match_time': self.edit_match_time_dropdown.selected_value,
      'player1': self.btd_playera1_dropdown.selected_value,
      'player2': self.btd_playera2_dropdown.selected_value,
      'player3': self.btd_playerb1_dropdown.selected_value,
      'player4': self.btd_playerb2_dropdown.selected_value,
      'ppr_playera1': self.ppr_playera1_dropdown.selected_value,
      'ppr_playera2': self.ppr_playera2_dropdown.selected_value,
      'ppr_playerb1': self.ppr_playerb1_dropdown.selected_value,
      'ppr_playerb2': self.ppr_playerb2_dropdown.selected_value,
    }

    if self.file_replaced:
      fields['csv_data'] = self.pending_stats['cleaned_csv']
      fields['filename'] = self.pending_stats['filename']
      fields['points'] = self.pending_stats['points']
      fields['completeness_score'] = self.pending_stats['completeness_score']
      fields['per_players'] = self.pending_stats['per_players']
      fields['per_xy'] = self.pending_stats['per_xy']
      fields['per_srv_players'] = self.pending_stats['per_srv_players']

    result = anvil.server.call('update_existing_btd_file', self.selected_file_id, fields)
    Notification(result['message'], style='success', timeout=4).show()

    self.load_files()
    self.select_file(self.selected_file_id)

  def delete_button_click(self, **event_args):
    if not self.selected_file_id:
      return

    if not confirm("Are you sure you want to DELETE this file?\n\nThis action cannot be undone."):
      return

    result = anvil.server.call('delete_btd_file', self.selected_file_id)
    Notification(result['message'], style='success', timeout=3).show()

    self.edit_card.visible = False
    self.select_file_label.visible = True
    self.selected_file_id = None
    self.selected_file_details = None
    self.load_files()

  def close_button_click(self, **event_args):
    open_form('Homepage.DataMgr')
