from ._anvil_designer import btd_importTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class btd_import(btd_importTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.
    # find out team for this user
    user_row = anvil.users.get_user()
    if not user_row:
      alert("Not Currently Logged in")
      open_form('Homepage.Landing_form')

    
    user_team = user_row["team"]
  
    # Now, let's populate the drop downs. 
    self.league_drop_down.selected_value = user_row["def_league"]
    self.gender_drop_down.selected_value = user_row["def_gender"]
    self.year_drop_down.selected_value = user_row["def_year"]
    self.league2_drop_down.selected_value = user_row["def_league"]
    self.gender2_drop_down.selected_value = user_row["def_gender"]
    self.year2_drop_down.selected_value = user_row["def_year"]

    #print(f"League, gender, year, selected values:{self.league_drop_down.selected_value}, {self.gender_drop_down.selected_value},{self.year_drop_down.selected_value} ")
  
    # populate the drop downs for league, and competition level 1 and 3
    self.league_drop_down.items = [(row["league"], row) for row in app_tables.league_list.search()]
    self.league2_drop_down.items = self.league_drop_down.items
    #print(f"League Drop Down Items {self.league_drop_down.items}")
    # for competition level 1
    self.comp_l1_drop_down.items = [(row["comp_l1"], row) for row in app_tables.league_comp_l1.search( league = self.league_drop_down.selected_value['league'] )]
    
    # for Competition Level 3, need to serach the selected league's playoff structure (flight, playoffs)
    self.comp_l3_drop_down.items = [(row["comp_l3"], row) for row in app_tables.league_comp_l3.search( comp_l3_label = self.league_drop_down.selected_value['comp_l3_label'])]
    
  pass

  def league_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    self.comp_l1_drop_down.items = [(row["comp_l1"], row) for row in app_tables.league_comp_l1.search( league = self.league_drop_down.selected_value['league'] )]

    #print(f"league drop down value:{self.league_drop_down.selected_value['league']}")
    if "NCAA" in self.league_drop_down.selected_value['league']:
      self.gender_drop_down.selected_value = "W"
      #print("changed gender to W")
      
    # for Competition Level 3, need to serach the selected league's playoff structure (flight, playoffs)
    self.comp_l3_drop_down.items = [(row["comp_l3"], row) for row in app_tables.league_comp_l3.search( comp_l3_label = self.league_drop_down.selected_value['comp_l3_label'])]
    self.league2_drop_down.selected_value = self.league_drop_down.selected_value
    pass

  def file_loader_1_change(self, file, **event_args):
    """This method is called when a new file is loaded into this FileLoader"""
     # calcuate the information on the uploaded btd file
    return_value =  anvil.server.call('update_btd_characteristics', file )

    print(f"players in the client, {return_value[0]} , {return_value[1]} , {return_value[2]} , {return_value[3]}")
    print(f"League Dropdown selected value:{self.league_drop_down.selected_value}")
    
    player_list = [ return_value[0], return_value[1], return_value[2], return_value[3] ]
    
    self.btd_playera1_drop_down.items = player_list
    self.btd_playera2_drop_down.items = player_list
    #self.btd_playerb1_drop_down.items = player_list
    self.btd_playerb2_drop_down.items = player_list
    self.btd_playerb1_drop_down.items = player_list

    self.btd_playera1_drop_down.selected_value = return_value[0]
    self.btd_playera2_drop_down.selected_value = return_value[1]
    self.btd_playerb1_drop_down.selected_value = return_value[2]
    self.btd_playerb2_drop_down.selected_value = return_value[3]

    # Set the drop down for the ppr players
    ppr_list = [
      {
        "team":   row["team"],
        "number":   row["number"],
        "shortname": row["shortname"],
      }
      for row in app_tables.master_player.search(
        league = self.league_drop_down.selected_value['league'],
        year = self.year_drop_down.selected_value,
        gender=self.gender_drop_down.selected_value
      )
    ]
    ppr_player_list = []
    for i in ppr_list:
      ppr_player_list.append( i['team']+" "+i['number']+" "+i['shortname'] )
    
    self.ppr_playera1_drop_down.items = ppr_player_list
    self.ppr_playera2_drop_down.items = ppr_player_list
    self.ppr_playerb1_drop_down.items = ppr_player_list
    self.ppr_playerb2_drop_down.items = ppr_player_list
    
    # save the other values
    self.points_label.text = return_value[4]
    self.comp_score_label.text = return_value[5]
    self.per_players_label.text = return_value[6]
    self.per_coord_label.text = return_value[7] 
    self.per_srv_player_label.text = return_value[8]
    self.filename_box.text = file.name
    pass

  def save_file_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    alert("Saving your data now")

    # pull out the correct selected value from the comp3 selected value row
    #print(f" self comp_l3 drop down selected value: {self.comp_l3_drop_down.selected_value}")
    #print(f"size of selected value row: {len(self.comp_l3_drop_down.selected_value)}")
    #print(f" selected value 0 {self.comp_l3_drop_down.selected_value[0]}")
    #print(f" selected value 1 {self.comp_l3_drop_down.selected_value[1]}")
    #print(f" selected value 2 {self.comp_l3_drop_down.selected_value[2]}")

    # the various statistics were converted to strings, %'s, so we need to convert them back to float.
    per_play = float(self.per_players_label.text[:-1])
    pts = int(self.points_label.text)
    per_s_play = float(self.per_srv_player_label.text[:-1])
    per_coord = float(self.per_coord_label.text[:-1])
    score = float(self.comp_score_label.text[:-1])
    if self.private_button.selected:
      is_private = True
    else:
      is_private = False
    
    # creat a new row with the data    
    app_tables.btd_files.add_row( 
      league=self.league_drop_down.selected_value["league"],
      gender=self.gender_drop_down.selected_value,
      year=self.year_drop_down.selected_value,
      comp_l1=self.comp_l1_drop_down.selected_value["comp_l1"],
      comp_l2=self.comp_l2_box.text,
      comp_l3=self.comp_l3_drop_down.selected_value["comp_l3"],
      date=self.date_picker.date,
      filename=self.file_loader_1.file.name,
      csv_data=self.file_loader_1.file,
      team=anvil.users.get_user()['team'],
      points = pts,
      per_players = per_play,
      per_srv_players = per_s_play,
      per_xy = per_coord,
      completeness_score = score,
      include_in_master = True,
      private = is_private,
      player1 = self.btd_playera1_drop_down.selected_value,
      player2 = self.btd_playera2_drop_down.selected_value,
      player3 = self.btd_playerb1_drop_down.selected_value,
      player4 = self.btd_playerb2_drop_down.selected_value,
      ppr_playera1 = self.ppr_playera1_drop_down.selected_value,
      ppr_playera2 = self.ppr_playera2_drop_down.selected_value,
      ppr_playerb1 = self.ppr_playerb1_drop_down.selected_value,
      ppr_playerb2 = self.ppr_playerb2_drop_down.selected_value
    )
    alert("New Row Saved!")
    open_form("Homepage.DataMgr")

  def private_help_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    alert("This defines how the data will be user: /n Private - Only avaiable to the team, used for Player and Pair reports. /n Public - used for scouting, avaialble to all teams on the Internals network.")
    pass

  def save_player_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    # this is to save a new player in the master player database.
    # First, check if the player exists
    p_rows = app_tables.master_player.search(
      league = self.league2_drop_down.selected_value['league'],
      gender = self.gender2_drop_down.selected_value,
      year=self.year2_drop_down.selected_value,
      team = self.team2_text_box.text,
      number=self.number_text_box.text,
      shortname=self.short_name_text_box.text
      )

    if len(p_rows) > 0:
      alert(title='Player Elready Exists')
      return False

    add_row = app_tables.master_player.add_row(
      league = self.league2_drop_down.selected_value['league'],
      gender = self.gender2_drop_down.selected_value,
      year=self.year2_drop_down.selected_value,
      team = self.team2_text_box.text,
      number=self.number_text_box.text,
      shortname=self.short_name_text_box.text,
      fullname=self.full_name_text_box.text
    )

    if add_row:
      alert(title='Player Added Successfully')

    # now update the drop down menus for players
    
    # Set the drop down for the ppr players
    ppr_list = [
      {
        "team":   row["team"],
        "number":   row["number"],
        "shortname": row["shortname"],
      }
      for row in app_tables.master_player.search(
        league = self.league_drop_down.selected_value['league'],
        year = self.year_drop_down.selected_value,
        gender=self.gender_drop_down.selected_value
      )
    ]
    ppr_player_list = []
    for i in ppr_list:
      ppr_player_list.append( i['team']+" "+i['number']+" "+i['shortname'] )
    
    self.ppr_playera1_drop_down.items = ppr_player_list
    self.ppr_playera2_drop_down.items = ppr_player_list
    self.ppr_playerb1_drop_down.items = ppr_player_list
    self.ppr_playerb2_drop_down.items = ppr_player_list
    pass

  def year_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    # set the add player year to the same year
    self.year2_drop_down.selected_value = self.year_drop_down.selected_value
    pass

  def gender_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    # set thegender to the same for the add player link
    self.gender2_drop_down.selected_value = self.gender_drop_down.selected_value
    pass

