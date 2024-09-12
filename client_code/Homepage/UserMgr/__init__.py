from ._anvil_designer import UserMgrTemplate
from anvil import *
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class UserMgr(UserMgrTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.

  def singout_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    user_row = anvil.users.get_user(allow_remembered=True)
    if user_row:
      if confirm("Logout of Beach Internals?"):
        anvil.users.logout()
    else:
      alert("Not Currently Logged in to Beach Internals")
    pass

  def signin_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    anvil.users.login_with_form()
    user_row = anvil.users.get_user(allow_remembered=True)
    if not user_row:
      alert("Login Failed")
    else:
      alert("Login Success")

    pass
