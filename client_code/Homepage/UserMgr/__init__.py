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
    # set the initial values:
    user_row = anvil.users.get_user()
    if user_row:
      self.email_label.text = user_row['email']
      self.label_3.text = user_row['team']
      self.label_role.text = user_row['role']
      self.admin_check_box.checked = user_row['client_admin']
      self.league_label.text = user_row['def_league']
      self.gender_label.text = user_row['def_gender']
      self.year_label.text = user_row['def_year']
    else:
      self.email_label.text = "Not Signed In"
      self.label_3.text = ""
      self.label_role.text = ""
      self.admin_check_box.checked = False
      self.league_label.text = ""
      self.gender_label.text = ""
      self.year_label.text = ""
      
  pass

  def singout_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    user_row = anvil.users.get_user(allow_remembered=True)
    if user_row:
      if confirm("Logout of Beach Internals?"):
        anvil.users.logout()
    else:
      alert("Not Currently Logged in to Beach Internals")

    # set the initial values:
    user_row = anvil.users.get_user()
    if user_row:
      self.email_label.text = user_row['email']
      self.label_3.text = user_row['team']
      self.label_role.text = user_row['role']
      self.admin_check_box.checked = user_row['client_admin']
      self.league_label.text = user_row['def_league']
      self.gender_label.text = user_row['def_gender']
      self.year_label.text = user_row['def_year']
    else:
      self.email_label.text = "Not Signed In"
      self.label_3.text = ""
      self.label_role.text = ""
      self.admin_check_box.checked = False
      self.league_label.text = ""
      self.gender_label.text = ""
      self.year_label.text = ""
      
      
    pass
  
  def signin_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    anvil.users.login_with_form()
    user_row = anvil.users.get_user(allow_remembered=True)
    if not user_row:
      alert("Login Failed")
    else:
      alert("Login Success")
      
    # set the initial values:
    user_row = anvil.users.get_user()
    if user_row:
      self.email_label.text = user_row['email']
      self.label_3.text = user_row['team']
      self.label_role.text = user_row['role']
      self.admin_check_box.checked = user_row['client_admin']
      self.league_label.text = user_row['def_league']
      self.gender_label.text = user_row['def_gender']
      self.year_label.text = user_row['def_year']
    else:
      self.email_label.text = "Not Signed In"
      self.label_3.text = ""
      self.label_role.text = ""
      self.admin_check_box.checked = False
      self.league_label.text = ""
      self.gender_label.text = ""
      self.year_label.text = ""
      
      
    pass
