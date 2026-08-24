from ._anvil_designer import btd_file_row_templateTemplate
from anvil import *
import anvil.server
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.users

class btd_file_row_template(btd_file_row_templateTemplate):
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

    # Filename
    self.filename_label.text = self.item['filename'] or "Untitled"

    # Error indicator
    no_errors = self.item['no_errors']
    if no_errors and no_errors > 0:
      self.error_icon.visible = True
      self.error_icon.text = f"⚠{no_errors}"
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

  def select_link_click(self, **event_args):
    """This method is called when the row's 'View / Correct' link is clicked"""
    # Bubble the selection up to the RepeatingPanel so the parent form can react
    self.parent.raise_event('x-select-file', item=self.item)
