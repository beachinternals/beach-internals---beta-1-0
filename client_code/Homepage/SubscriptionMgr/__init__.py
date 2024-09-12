from ._anvil_designer import SubscriptionMgrTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables



class SubscriptionMgr(SubscriptionMgrTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.
    # Check for login
    user_row = anvil.users.get_user(allow_remembered=True)
    if not user_row:
      alert('Please Sign In to Beach Internals')
      open_form('Homepage.UserMgr')
    elif not user_row["team"]:
      alert('Please Contact Beach Internals to be Assigned to a Team')
      open_form('Homepage.Contact')
    # set the initial values:

    if user_row:
      self.user_label.text = user_row['email']
      self.Label_team.text = user_row['team']
    else:
      self.user_label.text = "Not Logged In"
      self.Label_team.text = user_row['team']

    self.repeating_panel_1.items = app_tables.subscriptions.search(team=anvil.users.get_user()['team'])


  
  def SubsMgr1_click_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.outlined_card_3.clear()
    #self.outlined_card_3.add_component(btd_import())
    pass

  def SubsMgr2_click_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.outlined_card_3.clear()
    #self.outlined_card_3.add_component(btd_manage())
    pass


