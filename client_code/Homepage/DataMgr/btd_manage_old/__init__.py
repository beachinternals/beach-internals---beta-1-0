from ._anvil_designer import btd_manage_oldTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class btd_manage_old(btd_manage_oldTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    user_row = anvil.users.get_user()
    if not user_row:
      alert("Not Currently Logged in")
      open_form('Homepage.Landing_form')
    
    # Now, let's populate the drop downs. 
    self.league_drop_down.selected_value = user_row["def_league"]
    self.gender_drop_down.selected_value = user_row["def_gender"]
    self.year_drop_down.selected_value = user_row["def_year"]
 
    # populate the drop downs for league, and competition level 1 and 3
    self.league_drop_down.items = [(row["league"], row) for row in app_tables.league_list.search()]

    # for competition level 1
    self.comp_l1_drop_down.items = [(row["comp_l1"], row) for row in app_tables.league_comp_l1.search( league = self.league_drop_down.selected_value['league'] )]

    btd_rows = app_tables.btd_files.search(
      league=self.league_drop_down.selected_value['league'],
      gender=self.gender_drop_down.selected_value,
      year=self.year_drop_down.selected_value,
      comp_l1=self.comp_l1_drop_down.selected_value['comp_l1'], 
      team=self.team_label.text
    )
    btd_sorted = sorted( btd_rows, key=lambda btd_rows: btd_rows['no_errors'], reverse=True)
    self.repeating_panel_1.items = btd_sorted
    
    self.team_label.text = anvil.users.get_user()['team']
  pass
  
  def league_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    self.comp_l1_drop_down.items = [(row["comp_l1"], row) for row in app_tables.league_comp_l1.search( league = self.league_drop_down.selected_value['league'] )]

    #print(f"league drop down value:{self.league_drop_down.selected_value['league']}")
    if "NCAA" in self.league_drop_down.selected_value['league']:
      self.gender_drop_down.selected_value = "W"

    btd_rows = app_tables.btd_files.search(
      league=self.league_drop_down.selected_value['league'],
      gender=self.gender_drop_down.selected_value,
      year=self.year_drop_down.selected_value,
      comp_l1=self.comp_l1_drop_down.selected_value['comp_l1'],
      team=self.team_label.text
    )
    btd_sorted = sorted( btd_rows, key=lambda btd_rows: btd_rows['no_errors'], reverse=True)
    self.repeating_panel_1.items = btd_sorted
      
    pass

  def gender_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    btd_rows = app_tables.btd_files.search(
      league=self.league_drop_down.selected_value['league'],
      gender=self.gender_drop_down.selected_value,
      year=self.year_drop_down.selected_value,
      comp_l1=self.comp_l1_drop_down.selected_value['comp_l1'],
      team=self.team_label.text
    )
    btd_sorted = sorted( btd_rows, key=lambda btd_rows: btd_rows['no_errors'], reverse=True)
    self.repeating_panel_1.items = btd_sorted
    pass

  def year_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    btd_rows = app_tables.btd_files.search(
      league=self.league_drop_down.selected_value['league'],
      gender=self.gender_drop_down.selected_value,
      year=self.year_drop_down.selected_value,
      comp_l1=self.comp_l1_drop_down.selected_value['comp_l1'],
      team=self.team_label.text
    )
    btd_sorted = sorted( btd_rows, key=lambda btd_rows: btd_rows['no_errors'], reverse=True)
    self.repeating_panel_1.items = btd_sorted
    pass

  def comp_l1_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    btd_rows = app_tables.btd_files.search(
      league=self.league_drop_down.selected_value['league'],
      gender=self.gender_drop_down.selected_value,
      year=self.year_drop_down.selected_value,
      comp_l1=self.comp_l1_drop_down.selected_value['comp_l1'],
      team=self.team_label.text
    )
    btd_sorted = sorted( btd_rows, key=lambda btd_rows: btd_rows['no_errors'], reverse=True)
    self.repeating_panel_1.items = btd_sorted
    pass
