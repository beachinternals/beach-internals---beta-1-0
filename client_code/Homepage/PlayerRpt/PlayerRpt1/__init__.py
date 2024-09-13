from ._anvil_designer import PlayerRpt1Template
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class PlayerRpt1(PlayerRpt1Template):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.

    # Any code you write here will run before the form opens.
    # find out team for this user
    user_row = anvil.users.get_user()
    if not user_row:
      alert("Not Currently Logged in")
      open_form('Homepage.Landing_form')
  
    # Now, let's populate the drop downs. 
    self.league_drop_down.selected_value = user_row["def_league"]
    self.gender_drop_down.selected_value = user_row["def_gender"]
    self.year_drop_down.selected_value = user_row["def_year"]
    self.team_drop_down.selected_value = user_row["team"]


    #print(f"League, gender, year, selected values:{self.league_drop_down.selected_value}, {self.gender_drop_down.selected_value},{self.year_drop_down.selected_value} ")
  
    # populate the drop downs for league, and competition level 1 and 3
    self.league_drop_down.items = [(row["league"], row) for row in app_tables.league_list.search()]

    # competition level 1
    #self.comp_l1_drop_down.items = [(row["comp_l1"], row) for row in app_tables.league_comp_l1.search( league = self.league_drop_down.selected_value['league'] )]
    
    # Competition Level 3, need to serach the selected league's playoff structure (flight, playoffs)
    #self.comp_l3_drop_down.items = [(row["comp_l3"], row) for row in app_tables.league_comp_l3.search( comp_l3_label = self.league_drop_down.selected_value['comp_l3_label'])]
    
    # populate the drop down for teams, but we want unique items
    searchitem = list(set([(r['team']) for r in app_tables.btd_files.search()]))
    self.team_drop_down.items = searchitem

    # Player drop down
    self.player_drop_down.items = [(row['team']+" "+row['number']+" "+row['shortname'], row) for row in app_tables.master_player.search(
      league = self.league_drop_down.selected_value['league'],
      gender = self.gender_drop_down.selected_value,
      year=self.year_drop_down.selected_value)]

  def generate_report_button_click(self, **event_args):
    #"""This method is called when the button is clicked"""
    print(f"player selected value:{self.player_drop_down.selected_value}")
    player = self.player_drop_down.selected_value['team']+" "+self.player_drop_down.selected_value['number']+" "+self.player_drop_down.selected_value['shortname']
    table_markup = anvil.server.call('fbhe_table_query',
                    self.league_drop_down.selected_value,
                    self.gender_drop_down.selected_value,
                    self.year_drop_down.selected_value,
                    self.team_drop_down.selected_value,
                    player+"|")
    #now, save the markup to the self.?
    self.fbhe_table_markup.content = table_markup
    #print(table_markup)
    pass
