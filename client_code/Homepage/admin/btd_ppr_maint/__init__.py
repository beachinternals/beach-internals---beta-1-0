from ._anvil_designer import btd_ppr_maintTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class btd_ppr_maint(btd_ppr_maintTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.

    # populate the league pull down (both original and copy)
    # original is for creating ppr files from btd files, copy is for merging ppr into a master
    user_row = anvil.users.get_user()
    if not user_row:
      alert("Not Currently Logged in")
      open_form('Homepage.Landing_form')
  
    # Now, let's populate the drop downs. 
    self.league_drop_down.selected_value = user_row["def_league"]
    self.gender_drop_down.selected_value = user_row["def_gender"]
    self.year_drop_down.selected_value = user_row["def_year"]
    self.team_drop_down.selected_value = user_row['team']

    # populate the drop downs for league
    self.league_drop_down.items = [(row["league"], row) for row in app_tables.league_list.search()]

    # populate the drop down for teams, but we want unique items
    searchitem = list(set([(r['team']) for r in app_tables.btd_files.search()]))
    self.team_drop_down.items = searchitem

    # Now, let's populate the drop downs for the copy version! 
    self.league_drop_down_copy.selected_value = user_row["def_league"]
    self.gender_drop_down_copy.selected_value = user_row["def_gender"]
    self.year_drop_down_copy.selected_value = user_row["def_year"]
    self.team_drop_down_copy.selected_value = user_row['team']

    # populate the drop downs for league
    self.league_drop_down_copy.items = [(row["league"], row) for row in app_tables.league_list.search()]

    # populate the drop down for teams, but we want unique items
    searchitem = list(set([(r['team']) for r in app_tables.btd_files.search()]))
    self.team_drop_down_copy.items = searchitem

  def generate_ppr_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    pass

  def merge_ppr_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    pass

