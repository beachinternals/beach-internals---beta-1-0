from ._anvil_designer import PlayerRptTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from PlayerRpt1 import *
from PlayerRpt2 import *
from datetime import datetime



class PlayerRpt(PlayerRptTemplate):
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

    # First, populate the selected values
    self.league_drop_down.selected_value = user_row["def_league"]+'|'+user_row['def_gender']+'|'+user_row['def_year']
    self.league_drop_down.items = list(set([(r['league'])+' | '+r['gender']+' | '+r['year'] for r in app_tables.subscriptions.search(team=user_row['team'])]))

    # populate the drop downs for league, and competition level 1 and 3
    self.comp_l1_drop_down.items = [(row["comp_l1"], row) for row in app_tables.league_comp_l1.search( league = user_row["def_league"] )]
    self.comp_l2_drop_down.items = [(row['comp_l2'], row) for row in app_tables.league_comp_l2.search( league= user_row['def_league'] )]
    self.comp_l3_drop_down.items = [(row["comp_l3"], row) for row in app_tables.league_comp_l3.search( comp_l3_label = user_row['def_league'])]

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

    # set teh defulat to 'scouting'
    self.team_drop_down.selected_value = 'Scouting'
  
  def PlayerRpt1_click_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.outlined_card_3.clear()
    self.outlined_card_3.add_component(PlayerRpt1())
    pass

  def PlayerRpt2_click_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.outlined_card_3.clear()
    self.outlined_card_3.add_component(PlayerRpt2())
    pass

  def roster_manage_click_click(self, **event_args):
    """This method is called when the link is clicked"""
    self.outlined_card_3.clear()
    self.outlined_card_3.add_component(roster_manage())
    pass

  def comp_l1_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    # if we change the competition level 1, this will change the options for competition level 2
    
    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()
    
    # set comp_l2 data:    
    self.comp_l2_drop_down.items = [(row['comp_l2'], row) for row in app_tables.league_comp_l2.search( 
      league= disp_league,
      comp_l1=self.comp_l1_drop_down.selected_value['comp_l1']
    )]
    
    # set comp_l3 data:
    comp3lbl = [(r['comp_l3_label'],r) for r in app_tables.league_list.search(league=disp_league)]
    self.comp_l3_drop_down.items = [(row["comp_l3"], row) for row in app_tables.league_comp_l3.search( comp_l3_label = comp3lbl[0][0])]

    pass

  def league_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()

    # set comp_l1 data:
    self.comp_l1_drop_down.items = [(row["comp_l1"], row) for row in app_tables.league_comp_l1.search( league = disp_league )]

    # set comp_l2 data:
    self.comp_l2_drop_down.items = [(row['comp_l2'], row) for row in app_tables.league_comp_l2.search( 
      league= disp_league,
      comp_l1=self.comp_l1_drop_down.selected_value['comp_l1']
    )]
    
    # set comp_l3 data:
    comp3lbl = [(r['comp_l3_label'],r) for r in app_tables.league_list.search(league=disp_league)]
    self.comp_l3_drop_down.items = [(row["comp_l3"], row) for row in app_tables.league_comp_l3.search( comp_l3_label = comp3lbl[0][0])]

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

  def end_date_picker_change(self, **event_args):
    """This method is called when the selected date changes"""
    pass

  def generate_report_buttoon_click(self, **event_args):
    """This method is called when the button is clicked"""
    # unpack the league data:
        # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()

    # unpack the player
    disp_player = self.player_drop_down.selected_value['team'] + " "+self.player_drop_down.selected_value['number']+' '+self.player_drop_down.selected_value['shortname']+'|'

    # unpack the source data:
    user_row = anvil.users.get_user(allow_remembered=True)
    disp_team = user_row['team']

    # unpack the report to process
    # replace this with a data driven approach
    report_name = self.report_drop_down.selected_value
    scout = True      # right now, we always want the scouting version of the data
    if report_name == 'FBHE Along the Net':
      if self.team_drop_down.selected_value == "Scouting":
        function_name = 'fbhe_scout_query'
      else:
        function_name = 'fbhe_table_query'
    elif report_name == 'FBHE by Attack Tactic':
      function_name = 'fbhe_by_tactic'
    elif report_name == 'FBHE by Serve Source':
      function_name = 'fbhe_by_srv_src'

    # now, call the server module.
    # now including limits on competition (1,2,3) and dates
    print(f"calling server function:{function_name},{disp_league}, {disp_gender}, {disp_year},{disp_team}, {disp_player},{self.comp_l1_check_box.checked},{self.comp_l2_check_box.checked},{self.comp_l3_check_box.checked},{self.date_check_box.checked}")
    table_data = anvil.server.call(function_name, 
                                   disp_league, disp_gender, disp_year, 
                                   disp_team, disp_player, 
                                   self.comp_l1_check_box.checked, self.comp_l1_drop_down.selected_value['comp_l1'],
                                   self.comp_l2_check_box.checked, self.comp_l2_drop_down.selected_value['comp_l2'],
                                   self.comp_l3_check_box.checked, self.comp_l3_drop_down.selected_value['comp_l3'],
                                   self.date_check_box.checked, self.start_date_picker.date, self.end_date_picker.date,
                                   scout
                                  )

    # now put this into the rtf box
    filter_text = 'Data Filtered on: League='+disp_league+' Gender='+disp_gender+' Year='+disp_year
    filter_text = filter_text + ' Player=' + disp_player
    if self.comp_l1_check_box.checked:
      filter_text = filter_text + ' Competition1='+self.comp_l1_drop_down.selected_value['comp_l1']
    if self.comp_l2_check_box.checked:
      filter_text = filter_text + ' Competition2='+self.comp_l2_drop_down.selected_value['comp_l2']
    if self.comp_l3_check_box.checked:
      filter_text = filter_text + ' Competition3='+self.comp_l3_drop_down.selected_value['comp_l3']
    if self.date_check_box.checked:
      filter_text = filter_text + ' Date Range ='+str(self.start_date_picker.date)+' to '+str(self.end_date_picker.date)
      
    self.rich_text_2.content = filter_text
    self.rpt_disp_box.content = table_data
    
    pass
