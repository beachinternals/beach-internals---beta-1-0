from ._anvil_designer import HelpPageTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from video_upload_help import *
from running_report_help import *

class HelpPage(HelpPageTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.

  def video_upload_help_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.outlined_card_2.clear()
    self.outlined_card_2.add_component(video_upload_help())
    pass

  def link_1_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.outlined_card_2.clear()
    self.outlined_card_2.add_component(running_report_help())
    pass
