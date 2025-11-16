from ._anvil_designer import scheduled_tasksTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class scheduled_tasks(scheduled_tasksTemplate):
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

  def login_hist_click(self, **event_args):
    """This method is called when the button is clicked"""
    # call server evnetf
    return_text = anvil.server.call('call_chk_user_logins')
    # give an alert
    alert(return_text)
    pass

  def build_update_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    return_text = anvil.server.call('call_build_ppr_data')
    alert(return_text)
    pass

  def rebuild_all_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    return_text = anvil.server.call('call_build_all_ppr_data')
    alert(return_text)
    pass

  def outlined_button_3_click(self, **event_args):
    """This method is called when the button is clicked"""
    return_text = anvil.server.call('call_merge_ppr_data')
    alert(return_text)
    pass

  def outlined_button_4_click(self, **event_args):
    """This method is called when the button is clicked"""
    return_text = anvil.server.call('call_calculate_data')
    alert(return_text)
    pass

  def pair_file_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    return_text = anvil.server.call(('build_pair_table'))
    alert(return_text)
    pass

  def league_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    pass

  def calc_sw_click(self, **event_args):
    """This method is called when the button is clicked"""
    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()

    return_value = anvil.server.call('calc_sw_player_clientcallable', disp_league, disp_gender, disp_year )

    return return_value

  def night_processing_button_click(self, **event_args):
    """This method is called when the button is clicked"""
        # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()
    r_val = anvil.server.call('night_processing_callable',disp_league,disp_gender,disp_year,True,False)
    pass

  def night_proc_all_click(self, **event_args):
    """This method is called when the button is clicked"""
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()
    r_val = anvil.server.call('night_processing_callable',disp_league,disp_gender,disp_year,False,True)
  pass

  def calc_sw_pair_click(self, **event_args):
    """This method is called when the button is clicked"""
    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()

    return_value = anvil.server.call('calc_sw_pair_clientcallable', disp_league, disp_gender, disp_year )

    return return_value
    pass

  def outlined_button_5_click(self, **event_args):
    """This method is called when the button is clicked"""
    return_value = anvil.server.call('trigger_nightly_check')

    pass

  def button_1_click(self, **event_args):
    """This method is called when the button is clicked"""
    return_value = anvil.server.call('daily_performance_summary')
    pass

