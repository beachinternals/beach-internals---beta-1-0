from ._anvil_designer import SignInTemplate
from anvil import *
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class SignIn(SignInTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.

  def signing_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    # check if they are signed in, and if not, open the login screens
    user_row = anvil.users.get_user(allow_remembered=True)
    if not user_row:
      anvil.users.login_with_form()
      user_row = anvil.users.get_user(allow_remembered=True)
      if not user_row:
       alert("Login Failed")
      else:
       alert("Login Success")
    else:
      alert("Already Logged into Beach Internals")
      
    pass
