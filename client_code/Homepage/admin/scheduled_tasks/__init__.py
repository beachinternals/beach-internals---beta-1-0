from ._anvil_designer import scheduled_tasksTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class scheduled_tasks(scheduled_tasksTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.

  def login_hist_click(self, **event_args):
    """This method is called when the button is clicked"""
    # call server evnetf
    return_text = anvil.server.call('call_chk_user_logins')
    # give an alert
    alert(return_text)
    pass

