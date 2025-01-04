from ._anvil_designer import pdf_reportsTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.media


class pdf_reports(pdf_reportsTemplate):
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

  def outlined_button_1_click(self, **event_args):
    """This method is called when the button is clicked"""
    email = self.email_box.text
    text1 = self.text1_box.text
    
    
    if email and text1:
      alert(f'Thanks for registering! Your PDF is downloading and will be sent to {email}.')

      pdf = anvil.server.call('send_pdf_email', email, text1)
      anvil.media.download(pdf)

    else:
        alert('You have not completed all required fields')
      
    pass

  def gen_all_rpt_pdf_click(self, **event_args):
    """This method is called when the button is clicked"""
    # take the given (league+) player, and run thru the list and generate all reports, email to user, and download here

    # unpck the league, gender, and year
    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()

    # unpack the player
    disp_player = self.player_drop_down.selected_value['team'] + " "+self.player_drop_down.selected_value['number']+' '+self.player_drop_down.selected_value['shortname']

    # unpack the source data:
    user_row = anvil.users.get_user(allow_remembered=True)
    disp_team = user_row['team']

    table_data4 = ''
    scout = True
    
    return_value = anvil.server.call('render_all_rpts_pdf_callable',
                                   disp_league, disp_gender, disp_year, 
                                   disp_team, disp_player, 
                                   self.comp_l1_check_box.checked, self.comp_l1_drop_down.selected_value['comp_l1'],
                                   self.comp_l2_check_box.checked, self.comp_l2_drop_down.selected_value['comp_l2'],
                                   self.comp_l3_check_box.checked, self.comp_l3_drop_down.selected_value['comp_l3'],
                                   self.date_check_box.checked, self.start_date_picker.date, self.end_date_picker.date,
                                   scout, table_data4, 'player'
                                  )

    alert('PDF report running in background'+return_value)

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

    # set the player drop down
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




