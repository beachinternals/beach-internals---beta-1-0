from ._anvil_designer import player_data_mgmtTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class player_data_mgmt(player_data_mgmtTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.
    # Check for login
    user_row = anvil.users.get_user(allow_remembered=True)
    if not user_row:
      alert("Please Sign In to Beach Internals")
      open_form("Homepage.UserMgr")
    elif not user_row["team"]:
      alert("Please Contact Beach Internals to be Assigned to a Team")
      open_form("Homepage.Contact")

    # First, populate the selected values
    self.league_drop_down.selected_value = (
      user_row["def_league"] + "|" + user_row["def_gender"] + "|" + user_row["def_year"]
    )
    self.league_drop_down.items = list(
      set(
        [
          (r["league"]) + " | " + r["gender"] + " | " + r["year"]
          for r in app_tables.subscriptions.search(team=user_row["team"])
        ]
      )
    )

  def gen_uuid_click(self, **event_args):
    """This method is called when the button is clicked"""
    # call server evnetf
    return_text = anvil.server.call("backfill_player_uuids")
    # give an alert
    alert(return_text)
    pass



  def league_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    pass

