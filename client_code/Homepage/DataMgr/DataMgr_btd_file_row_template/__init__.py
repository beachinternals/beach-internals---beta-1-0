from ._anvil_designer import btd_file_row_templateTemplate
from anvil import *
import anvil.server
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.users

class DataMgr_btd_file_row_template(btd_file_row_templateTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.

    # Set up the display based on the item data
    if self.item:
      self.update_display()

  def update_display(self):
    """Update the display labels based on item data"""

    # Format the date
    if self.item['date']:
      self.date_label.text = self.item['date'].strftime('%m/%d')
    else:
      self.date_label.text = "No Date"

    # Competition (L1 - L2)
    comp_text = self.item['comp_l1'] or ''
    if self.item['comp_l2']:
      comp_text += f" - {self.item['comp_l2']}"
    self.comp_label.text = comp_text if comp_text else "No Competition"

    # Players (showing ppr mappings if available, otherwise btd players)
    if self.item['ppr_playera1'] and self.item['ppr_playerb1']:
      # Extract just the last names for brevity
      team_a = self.extract_last_name(self.item['ppr_playera1'])
      team_b = self.extract_last_name(self.item['ppr_playerb1'])
      self.players_label.text = f"{team_a} vs {team_b}"
    else:
      team_a = self.extract_last_name(self.item['player1'])
      team_b = self.extract_last_name(self.item['player3'])
      self.players_label.text = f"{team_a} vs {team_b}"

    # Completeness score
    score = self.item['completeness_score']
    if score is not None:
      self.comp_score_label.text = f"{score:.1f}%"

      # Color code the score
      if score >= 80:
        self.comp_score_label.foreground = '#28a745'  # Green
      elif score >= 60:
        self.comp_score_label.foreground = '#ffc107'  # Yellow
      else:
        self.comp_score_label.foreground = '#dc3545'  # Red
    else:
      self.comp_score_label.text = "N/A"

    # XY percentage
    per_xy = self.item['per_xy']
    if per_xy is not None:
      self.xy_label.text = f"XY: {per_xy:.1f}%"
    else:
      self.xy_label.text = "XY: N/A"

    # Points
    points = self.item['points']
    if points is not None:
      self.points_label.text = f"Pts: {points}"
    else:
      self.points_label.text = "Pts: 0"

    # Error indicator
    no_errors = self.item['no_errors']
    if no_errors and no_errors > 0:
      self.error_icon.visible = True
      self.error_icon.text = "⚠"
      self.error_icon.foreground = '#dc3545'
      self.error_icon.tooltip = f"{no_errors} error(s) found"
    else:
      self.error_icon.visible = False

    # Status indicator (Private vs Scouting)
    if self.item['private']:
      self.status_label.text = "🔒"
      self.status_label.tooltip = "Private"
    else:
      self.status_label.text = "👁"
      self.status_label.tooltip = "Scouting"

  def extract_last_name(self, full_name):
    """Extract last name from 'TEAM ## Name' format"""
    if not full_name:
      return "Unknown"

    # Split by spaces and get the last part
    parts = str(full_name).split()
    if len(parts) >= 3:
      # Format is typically: TEAM NUMBER NAME
      return parts[-1]
    elif len(parts) > 0:
      return parts[-1]
    else:
      return "Unknown"

  def form_click(self, **event_args):
    """This method is called when the form is clicked"""
    # Raise an event to the parent form that this row was clicked
    self.raise_event('x-select-file', file_data=self.item)

    # Highlight this row
    self.card_1.role = 'elevated-card'

  def card_1_show(self, **event_args):
    """This method is called when the column panel is shown on the screen"""
    pass
