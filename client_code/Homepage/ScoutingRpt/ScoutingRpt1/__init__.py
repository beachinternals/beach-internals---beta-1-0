from ._anvil_designer import ScoutingRpt1Template
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class ScoutingRpt1(ScoutingRpt1Template):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.

    # Any code you write here will run before the form opens.
    # find out team for this user
    user_row = anvil.users.get_user()
    if not user_row:
      alert("Not Currently Logged in")
      open_form("Homepage.Landing_form")

    # First, populate the selected values
    self.league_drop_down.selected_value = user_row["def_league"]+'|'+user_row['def_gender']+'|'+user_row['def_year']
    self.league_drop_down.items = list(set([(r['league'])+' | '+r['gender']+' | '+r['year'] for r in app_tables.subscriptions.search(team=user_row['team'])]))

    # populate the player drop down
    self.player_drop_down.items = [
      (row["team"] + " " + row["number"] + " " + row["shortname"], row)
      for row in app_tables.master_player.search(
        tables.order_by("team"),
        league=user_row['def_league'],
        gender=user_row['def_gender'],
        year=user_row['def_year'],
      )
    ]

  def generate_report_button_click(self, **event_args):
    # """This method is called when the button is clicked"""
    print(f"player selected value:{self.player_drop_down.selected_value}")
    player = (
      self.player_drop_down.selected_value["team"]
      + " "
      + self.player_drop_down.selected_value["number"]
      + " "
      + self.player_drop_down.selected_value["shortname"]
    )

    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()
    
    print(f"Player:{player}")
    table_markup = anvil.server.call(
      "fbhe_scout_query",
      disp_league,
      disp_gender,
      disp_year,
      anvil.users.get_user()['team'],
      player + "|",
    )
    # now, save the markup to the self.?
    self.fbhe_table_markup.content = table_markup
    # print(table_markup)
    pass

  def league_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    # UIpdate the Player drop down

    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()
    
    self.player_drop_down.items = [
      (row["team"] + " " + row["number"] + " " + row["shortname"], row)
      for row in app_tables.master_player.search(
        tables.order_by("team"),
        league=disp_league,
        gender=disp_gender,
        year=disp_year,
      )
    ]
    pass
