from ._anvil_designer import ScoutingRptTemplate
from anvil import *
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from ScoutingRpt1 import *
from ScoutingRpt2 import *



class ScoutingRpt(ScoutingRptTemplate):
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
      
  def ScoutingRpt1_click_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.outlined_card_3.clear()
    self.outlined_card_3.add_component(ScoutingRpt1())
    pass

  def ScoutingRpt2_click_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.outlined_card_3.clear()
    self.outlined_card_3.add_component(ScoutingRpt2())
    pass

  def roster_manage_click_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.outlined_card_3.clear()
    self.outlined_card_3.add_component(roster_manage())
    pass
