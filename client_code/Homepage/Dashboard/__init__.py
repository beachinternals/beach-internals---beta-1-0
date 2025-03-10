from ._anvil_designer import DashboardTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import datetime

class Dashboard(DashboardTemplate):
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
    else:
      # set up the league drop down
      self.league_drop_down.selected_value = user_row["def_league"]+'|'+user_row['def_gender']+'|'+user_row['def_year']
      self.league_drop_down.items = list(set([(r['league'])+' | '+r['gender']+' | '+r['year'] for r in app_tables.subscriptions.search(team=user_row['team'])]))

    # populate the reports drop down
    if anvil.users.get_user()['team'] == "INTERNALS":
      self.report_drop_down.items = [
      (row['report_name']) for row in app_tables.report_list.search(rpt_type='dashboard')]
    else:
      self.report_drop_down.items = [
      (row['report_name']) for row in app_tables.report_list.search(private=False, rpt_type='dashboard')
    ]
  
  
  def gen_dashboard_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    # here we go to the server and return a rich text table to display below
    user_row = anvil.users.get_user(allow_remembered=True)
    if not user_row:
      alert('Please Sign In to Beach Internals')
      open_form('Homepage.UserMgr')
    elif not user_row["team"]:
      alert('Please Contact Beach Internals to be Assigned to a Team')
      open_form('Homepage.Contact')
      
    # unpack the league data:
    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index('|')
    disp_league = league_value[:str_loc-1].strip()
    league_value = league_value[str_loc+1:]
    str_loc = league_value.index('|')
    disp_gender = league_value[:str_loc-1].strip()
    disp_year = league_value[str_loc+1:].strip()

    # unpack the source data:
    user_row = anvil.users.get_user(allow_remembered=True)
    disp_team = user_row['team']

    # unpack the report to process
    # replace this with a data driven approach
    rpt_name = self.report_drop_down.selected_value
    function_list = [(f_row['function_name']) for f_row in app_tables.report_list.search(report_name=rpt_name)]
    text_list = [(f_row['explain_text']) for f_row in app_tables.report_list.search(report_name=rpt_name)]
    box1_title_list = [(f_row['box1_title']) for f_row in app_tables.report_list.search(report_name=rpt_name)]
    box2_title_list = [(f_row['box2_title']) for f_row in app_tables.report_list.search(report_name=rpt_name)]
    box3_title_list = [(f_row['box3_title']) for f_row in app_tables.report_list.search(report_name=rpt_name)]
    #print(function_list)
    fnct_name = function_list[0]
    explain_text = text_list[0]
    box1_title = box1_title_list[0]
    box2_title = box2_title_list[0]
    box3_title = box3_title_list[0]
    scout = True
    
    # now, call the server module.
    # now including limits on competition (1,2,3) and dates
    # check comp_l3, if not, set to str()
    if type(self.comp_l3_drop_down.selected_value['comp_l3']) == type(None):
      self.comp_l3_drop_down.selected_value['comp_l3'] = str()
    
    disp_pair = '' # this is a dummy for player reports to keep the calling arguments consistent for player and pair reports
    disp_player = ''
    
    # call the server function
    table_data1, table_data2, table_data3 = anvil.server.call(fnct_name, 
                                   disp_league, disp_gender, disp_year, 
                                   disp_team, disp_pair, disp_player, 
                                   self.comp_l1_check_box.checked, self.comp_l1_drop_down.selected_value['comp_l1'],
                                   self.comp_l2_check_box.checked, self.comp_l2_drop_down.selected_value['comp_l2'],
                                   self.comp_l3_check_box.checked, self.comp_l3_drop_down.selected_value['comp_l3'],
                                   self.date_check_box.checked, self.start_date_picker.date, self.end_date_picker.date,
                                   scout, explain_text
                                  )

    # now put this into the rtf box
    filter_text = f"""
    Data Filters:
    - PDF Created : {datetime.datetime.today().strftime('%Y-%m-%d')}
    - League : {disp_league}
    - Gender : {disp_gender}
    - Year : {disp_year}
    - Player : {disp_player}
    - Competition 1 : {self.comp_l1_drop_down.selected_value['comp_l1'] if self.comp_l1_check_box.checked else ''}
    - Competition 2 : {self.comp_l2_drop_down.selected_value['comp_l2'] if self.comp_l2_check_box.checked else ''}
    - Competition 3 : {self.comp_l3_drop_down.selected_value['comp_l3'] if self.comp_l3_check_box.checked else ''}
    - Date Filtered : {str(self.start_date_picker.date)+' to '+str(self.end_date_picker.date) if self.date_check_box.checked else ''}
    """

    self.rich_text_2.content = filter_text
    self.rpt_disp_box1.content = table_data1
    self.rpt_disp_box2.content = table_data2
    self.rpt_disp_box3.content = table_data3
    self.rpt_disp_box4.content = explain_text
    self.box1_label.text = box1_title
    self.box2_label.text = box2_title
    self.box3_label.text = box3_title
    self.rpt_title.text = rpt_name
    #self.player_label.text = disp_player
        
    pass

  def league_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    """This method is called when an item is selected"""
    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index("|")
    disp_league = league_value[: str_loc - 1].strip()
    league_value = league_value[str_loc + 1 :]
    str_loc = league_value.index("|")
    disp_gender = league_value[: str_loc - 1].strip()
    disp_year = league_value[str_loc + 1 :].strip()

    # set comp_l1 data:
    self.comp_l1_drop_down.items = [
      (row["comp_l1"], row)
      for row in app_tables.league_comp_l1.search(league=disp_league)
    ]

    # set comp_l2 data:
    self.comp_l2_drop_down.items = [
      (row["comp_l2"], row)
      for row in app_tables.league_comp_l2.search(
        league=disp_league, comp_l1=self.comp_l1_drop_down.selected_value["comp_l1"]
      )
    ]

    # set comp_l3 data:
    comp3lbl = [
      (r["comp_l3_label"], r) for r in app_tables.league_list.search(league=disp_league)
    ]
    self.comp_l3_drop_down.items = [
      (row["comp_l3"], row)
      for row in app_tables.league_comp_l3.search(comp_l3_label=comp3lbl[0][0])
    ]


    
    # now find the players
    #disp_player1, disp_player2 = anvil.server.call('pair_players',self.pair_drop_down.selected_value['pair'])
    #self.player_drop_down.items = [disp_player1,disp_player2]
    
    # set the sstart and end date to beginning and end of the season (needed for function call)
    # self.start_date_picker.date = datetime.date.today()  # temp : need to pull from league DB
    # self.end_date_picker.date = datetime.date.today()

    pass

  def gen_pdf_rpt_click(self, **event_args):
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
    disp_player = ''
    
    # unpack the source data:
    user_row = anvil.users.get_user(allow_remembered=True)
    disp_team = user_row['team']

    # unpack the report to process
    # replace this with a data driven approach
    rpt_name = self.report_drop_down.selected_value
    function_list = [(f_row['function_name']) for f_row in app_tables.report_list.search(report_name=rpt_name)]
    text_list = [(f_row['explain_text']) for f_row in app_tables.report_list.search(report_name=rpt_name)]
    form_list = [(f_row['rpt_form']) for f_row in app_tables.report_list.search(report_name=rpt_name)]
    #print(function_list)
    fnct_name = function_list[0]
    table_data4 = text_list[0]
    form = form_list[0]
    scout = True
    
    # now, call the server module.
    # now including limits on competition (1,2,3) and dates
    # check comp_l3, if not, set to str()
    if type(self.comp_l3_drop_down.selected_value['comp_l3']) == type(None):
      self.comp_l3_drop_down.selected_value['comp_l3'] = str()

    disp_pair = '' # this is a dummy for player reports to keep the calling arguments consistent for player and pair reports
    # call the server function
    pdf_rpt = anvil.server.call('create_dashboard_pdf_reports', fnct_name, form,
                                   disp_league, disp_gender, disp_year, 
                                   disp_team, disp_pair, disp_player, 
                                   self.comp_l1_check_box.checked, self.comp_l1_drop_down.selected_value['comp_l1'],
                                   self.comp_l2_check_box.checked, self.comp_l2_drop_down.selected_value['comp_l2'],
                                   self.comp_l3_check_box.checked, self.comp_l3_drop_down.selected_value['comp_l3'],
                                   self.date_check_box.checked, self.start_date_picker.date, self.end_date_picker.date,
                                   scout, table_data4
                                  )
    result = anvil.server.call('send_email',"Beach Internals Player Report - PDF Version", 'Attached please find the PDF version of the Player Report', pdf_rpt, '', '' )    
    alert(('PDF report emailed'+str(result)))
    anvil.media.download(pdf_rpt)    
  
    pass
