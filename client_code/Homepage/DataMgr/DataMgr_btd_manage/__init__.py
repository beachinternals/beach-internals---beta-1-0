from ._anvil_designer import btd_manageTemplate
from anvil import *
import anvil.server
import anvil.users
import anvil.tables as tables
from anvil.tables import app_tables
import datetime

class btd_manage(btd_manageTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    # Check user
    user = anvil.users.get_user()
    if not user:
      alert("Not Currently Logged in")
      open_form('Homepage.Landing_form')
      return

    # Store user info
    self.user_team = user["team"]
    self.user_league = user["def_league"]
    self.user_gender = user["def_gender"]
    self.user_year = user["def_year"]

    # Currently selected file
    self.selected_file_id = None

    # Initialize
    self.init_form()

  def init_form(self):
    """Initialize the form"""

    # Populate league dropdown
    subscriptions = app_tables.subscriptions.search(team=self.user_team)
    league_items = []
    for sub in subscriptions:
      lgy = f"{sub['league']} | {sub['gender']} | {sub['year']}"
      league_items.append(lgy)

    league_items = list(set(league_items))
    league_items.sort(reverse=True)
    self.league_dropdown.items = league_items

    # Set default
    default_lgy = f"{self.user_league} | {self.user_gender} | {self.user_year}"
    if default_lgy in league_items:
      self.league_dropdown.selected_value = default_lgy
    elif len(league_items) > 0:
      self.league_dropdown.selected_value = league_items[0]

    # Team dropdown
    self.team_dropdown.items = [self.user_team]
    self.team_dropdown.selected_value = self.user_team

    # Sort dropdown
    self.sort_dropdown.items = [
      ('Date (Newest First)', 'date'),
      ('Completeness Score', 'completeness'),
      ('% with X,Y', 'per_xy'),
      ('Errors', 'errors')
    ]
    self.sort_dropdown.selected_value = 'date'

    # Load files
    self.load_files()

  def parse_lgy(self, lgy_string):
    """Parse 'NCAA | W | 2025' into components"""
    if not lgy_string:
      return None, None, None

    parts = lgy_string.split('|')
    if len(parts) != 3:
      return None, None, None

    return parts[0].strip(), parts[1].strip(), int(parts[2].strip())

  def load_files(self):
    """Load file list"""

    Notification("Loading files...").show()

    # Build filters
    filters = {}

    if self.league_dropdown.selected_value:
      league, gender, year = self.parse_lgy(self.league_dropdown.selected_value)
      if league:
        filters['league'] = league
        filters['gender'] = gender
        filters['year'] = year

    if self.team_dropdown.selected_value:
      filters['team'] = self.team_dropdown.selected_value

    if self.date_from_picker.date:
      filters['date_from'] = self.date_from_picker.date
    if self.date_to_picker.date:
      filters['date_to'] = self.date_to_picker.date

    if self.min_completeness_box.text:
      try:
        filters['min_completeness'] = float(self.min_completeness_box.text)
      except:
        pass

    if self.has_errors_checkbox.checked:
      filters['has_errors'] = True

    filters['sort_by'] = self.sort_dropdown.selected_value
    filters['sort_desc'] = True

    try:
      files = anvil.server.call('get_btd_files_filtered', filters)

      # Add display field for players
      for f in files:
        if f['ppr_playera1'] and f['ppr_playerb1']:
          a = f['ppr_playera1'].split()[-1] if f['ppr_playera1'] else 'Unknown'
          b = f['ppr_playerb1'].split()[-1] if f['ppr_playerb1'] else 'Unknown'
          f['players_display'] = f"{a} vs {b}"
        else:
          f['players_display'] = "Not mapped"

      self.file_grid.items = files
      self.file_count_label.text = f"{len(files)} files found"

    except Exception as e:
      alert(f"Error loading files: {str(e)}")

  def file_grid_row_click(self, row_data, **event_args):
    """When user clicks a file row"""

    if not row_data:
      return

    self.selected_file_id = row_data['id']

    try:
      details = anvil.server.call('get_btd_file_details', self.selected_file_id)

      if not details:
        alert("Error loading file details")
        return

      # Update labels
      self.file_name_label.text = details['filename']

      if details['date']:
        self.date_label.text = f"Date: {details['date'].strftime('%B %d, %Y')}"
      else:
        self.date_label.text = "Date: Not set"

      comp = f"{details['comp_l1'] or 'N/A'} - {details['comp_l2'] or 'N/A'}"
      if details['comp_l3']:
        comp += f" - {details['comp_l3']}"
      self.comp_label.text = f"Competition: {comp}"

      self.team_label.text = f"Team: {details['team']}"

      # Statistics
      stats = f"Points: {details['points'] or 0}\n"
      stats += f"Completeness: {details['completeness_score']:.1f}%\n" if details['completeness_score'] else "Completeness: N/A\n"
      stats += f"% Actions with Players: {details['per_players']:.1f}%\n" if details['per_players'] else "% Actions with Players: N/A\n"
      stats += f"% Serves with Players: {details['per_srv_players']:.1f}%\n" if details['per_srv_players'] else "% Serves with Players: N/A\n"
      stats += f"% with X,Y: {details['per_xy']:.1f}%" if details['per_xy'] else "% with X,Y: N/A"
      self.stats_label.text = stats

      # Players
      players = ""
      if details['player1'] and details['player2']:
        players += f"Team A: {details['player1']}, {details['player2']}\n"
      if details['player3'] and details['player4']:
        players += f"Team B: {details['player3']}, {details['player4']}\n"
      if details['ppr_playera1']:
        players += f"\nMapped to:\n"
        players += f"Team A: {details['ppr_playera1']}, {details['ppr_playera2']}\n"
        players += f"Team B: {details['ppr_playerb1']}, {details['ppr_playerb2']}"
      self.players_label.text = players if players else "No players detected"

      # Errors
      if details['no_errors'] and details['no_errors'] > 0:
        self.errors_label.visible = True
        self.errors_label.text = f"⚠ {details['no_errors']} error(s): {details['error_str']}"
      else:
        self.errors_label.visible = False

      # Show details panel
      self.select_file_label.visible = False
      self.details_card.visible = True

    except Exception as e:
      alert(f"Error: {str(e)}")

  def apply_filters_button_click(self, **event_args):
    """Apply filters"""
    self.load_files()

  def clear_filters_button_click(self, **event_args):
    """Clear filters"""
    self.date_from_picker.date = None
    self.date_to_picker.date = None
    self.min_completeness_box.text = ""
    self.has_errors_checkbox.checked = False
    self.load_files()

  def sort_dropdown_change(self, **event_args):
    """Sort changed"""
    self.load_files()

  def delete_button_click(self, **event_args):
    """Delete file"""

    if not self.selected_file_id:
      return

    if not confirm("Are you sure you want to DELETE this file?\n\nThis action cannot be undone."):
      return

    try:
      result = anvil.server.call('delete_btd_file', self.selected_file_id)

      if result['success']:
        Notification("File deleted successfully", style='success').show()
        self.details_card.visible = False
        self.select_file_label.visible = True
        self.selected_file_id = None
        self.load_files()
      else:
        alert(f"Error: {result['message']}")

    except Exception as e:
      alert(f"Error: {str(e)}")

  def close_button_click(self, **event_args):
    """Close form"""
    open_form('Homepage.DataMgr')
