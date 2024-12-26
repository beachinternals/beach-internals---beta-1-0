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

    # First, populate the selected values for the 3rd box
    self.league3_drop_down.selected_value = user_row["def_league"]+'|'+user_row['def_gender']+'|'+user_row['def_year']
    self.league3_drop_down.items = list(set([(r['league'])+' | '+r['gender']+' | '+r['year'] for r in app_tables.subscriptions.search(team=user_row['team'])]))
    self.league4_drop_down.selected_value = user_row["def_league"]+'|'+user_row['def_gender']+'|'+user_row['def_year']
    self.league4_drop_down.items = list(set([(r['league'])+' | '+r['gender']+' | '+r['year'] for r in app_tables.subscriptions.search(team=user_row['team'])]))

    
    # populate drop down for Traingle Scoring
      # populate the drop downs for league
    self.tri_league_drop_down.selected_value = user_row["def_league"]+'|'+user_row['def_gender']+'|'+user_row['def_year']
    self.tri_league_drop_down.items = list(set([(r['league'])+' | '+r['gender']+' | '+r['year'] for r in app_tables.subscriptions.search(team=user_row['team'])]))  

  def generate_ppr_button_click(self, **event_args):
    """This method is called when the button is clicked"""

    # call the serer function to convert btd to ppr files and then store them back in the btd_files data table
    return_text = anvil.server.call("make_ppr_files",
                      self.league_drop_down.selected_value['league'],
                      self.gender_drop_down.selected_value,
                      self.year_drop_down.selected_value,
                      self.team_drop_down.selected_value,
                      self.rebuild_button.selected
                     )
    #generate_ppr_files( user_league, user_gender, user_year,user_team):
    alert(return_text)
    pass

  def merge_ppr_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    # call the server function to do the merge
    user_league = self.league_drop_down_copy.selected_value['league']
    user_gender = self.gender_drop_down_copy.selected_value
    user_year = self.year_drop_down_copy.selected_value
    user_team = self.team_drop_down_copy.selected_value
    data_set = self.drop_down_data_set_copy.selected_value  # this should be: All, Private, Scouting, League
    
    return_text = anvil.server.call('create_master_ppr',user_league, user_gender, user_year, user_team, data_set )
    alert(return_text)
    pass

  def create_summary_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    
    # extract league, gender, year from league selected value
    league_value = self.league3_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()
    
    return_text = anvil.server.call(
      'calc_player_data_background',
      disp_league,
      disp_gender,
      disp_year
    )
    alert(return_text)
    pass

  def league_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    pass

  def league3_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
        # extract league, gender, year from league selected value
    league_value = self.league3_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()
    pass

  def all_player_data_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    return_text = anvil.server.call('calc_all_player_data')
    
    pass

  def tri_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    # Call the server routine to calculate the triangle scoring table

    # unpack the league, gender, year from the drop down
    # extract league, gender, year from league selected value
    league_value = self.tri_league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()
    
    return_text = anvil.server.call(
      'calc_triangle_scoring_background',
      disp_league,
      disp_gender,
      disp_year
    )
    alert(return_text)

    # call the server functions

    
    pass

  def create_pair_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    # Call the server routine to calculate the pair table

    # unpack the league, gender, year from the drop down
    # extract league, gender, year from league selected value
    league_value = self.league4_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()
    
    return_text = anvil.server.call(
      'build_pair_table',
      disp_league,
      disp_gender,
      disp_year
    )
    alert(return_text)
    pass

  def tri_league_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    pass

  def league4_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    pass

  def build_master_pair_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    task = anvil.server.call('build_pair_data_table')
    alert(task)
    pass

