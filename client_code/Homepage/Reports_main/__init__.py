from ._anvil_designer import Reports_mainTemplate
from anvil.js import window
import anvil.js
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import datetime
from anvil import open_form
import anvil.js



class Reports_main(Reports_mainTemplate):
  def __init__(self, **properties):
    #print(f"properties: {properties}")
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Check query parameters on load (optional, if not using StartupForm)
    url_params = anvil.js.window.location.search
    params = anvil.js.window.URLSearchParams(url_params)  # Corrected from .new
    form_name = params.get('Reports_main')
    report_id = params.get('report_id')

    # expand this to any form in the list in the report_list
    #form_list = [row['rpt_form'] for row in app_tables.report_list.search()]
    #print(f"report main, form name: {form_name}")
    #print(f'report main, form list: {form_list}')
    #if form_name in form_list and report_id:
    #  open_form(form_name, report_id=report_id)
      
    
    # self.all_rpt_pdf.visible = True if anvil.users.get_user()['team'] == "INTERNALS" else False

    # Any code you write here will run before the form opens.
    # Check for login
    user_row = anvil.users.get_user(allow_remembered=True)
    if not user_row:
      alert("Please Sign In to Beach Internals")
      open_form("Homepage.UserMgr")
    elif not user_row["team"]:
      alert("Please Contact Beach Internals to be Assigned to a Team")
      open_form("Homepage.Contact")

    disp_league = user_row["def_league"]
    # First, populate the selected values
    self.league_drop_down.selected_value = (
      user_row["def_league"] + "|" + user_row["def_gender"] + "|" + user_row["def_year"]
    )
    self.league_drop_down.items = list(
      set(
        [
          (r["league"]) + " | " + r["gender"] + " | " + r["year"]
          for r in app_tables.subscriptions.search(team=user_row["team"])
        ]
      )
    )


    
    # populate the drop downs for league, and competition level 1 and 3
    self.comp_l1_drop_down.items = [
      (row["comp_l1"], row)
      for row in app_tables.league_comp_l1.search(league=disp_league)
    ]
    self.comp_l2_drop_down.items = [
      (row["comp_l2"], row)
      for row in app_tables.league_comp_l2.search(league=disp_league)
    ]

    # set comp_l3 data:
    comp3lbl = [
      (r["comp_l3_label"], r)
      for r in app_tables.league_list.search(league=user_row["def_league"])
    ]
    self.comp_l3_drop_down.items = [
      (row["comp_l3"], row)
      for row in app_tables.league_comp_l3.search(comp_l3_label=comp3lbl[0][0])
    ]

    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index("|")
    disp_league = league_value[: str_loc - 1].strip()
    league_value = league_value[str_loc + 1 :]
    str_loc = league_value.index("|")
    disp_gender = league_value[: str_loc - 1].strip()
    disp_year = league_value[str_loc + 1 :].strip()

    # populate the player drop down
    self.player_drop_down.items = [
      (row["team"] + " " + row["number"] + " " + row["shortname"], row)
      for row in app_tables.master_player.search(
        tables.order_by("team"),
        tables.order_by("number"),
        league=disp_league,
        gender=disp_gender,
        year=disp_year,
      )
    ]

    # populate the report type drop down
    if anvil.users.get_user()["team"] == "INTERNALS":
      self.rpt_type_drop_down.items = sorted(
        list(set(row['rpt_type'] for row in app_tables.report_list.search() if row['rpt_type'] is not None))
      )
    else:
      self.rpt_type_drop_down.items = sorted(
        list(set(row['rpt_type'] for row in app_tables.report_list.search(private=False) if row['rpt_type'] is not None))
      )

    # populate the reports drop down
    if anvil.users.get_user()["team"] == "INTERNALS":
      self.report_drop_down.items = [
        (row["report_name"]) for row in app_tables.report_list.search(rpt_type="player")
      ]
    else:
      self.report_drop_down.items = [
        (row["report_name"])
        for row in app_tables.report_list.search(private=False, rpt_type="player")
      ]

  def comp_l1_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    # if we change the competition level 1, this will change the options for competition level 2

    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index("|")
    disp_league = league_value[: str_loc - 1].strip()
    league_value = league_value[str_loc + 1 :]
    str_loc = league_value.index("|")
    disp_gender = league_value[: str_loc - 1].strip()
    disp_year = league_value[str_loc + 1 :].strip()

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

    # populate the player drop down
    self.pair_drop_down.items = [
      (row["pair"], row)
      for row in app_tables.master_pair.search(
        tables.order_by("pair"),
        league=disp_league,
        gender=disp_gender,
        year=disp_year,
      )
    ]
    
    pass

  def league_drop_down_change(self, **event_args):
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

    # set the player drop down
    self.player_drop_down.items = [
      (row["team"] + " " + row["number"] + " " + row["shortname"], row)
      for row in app_tables.master_player.search(
        tables.order_by("team"),
        tables.order_by("number"),
        league=disp_league,
        gender=disp_gender,
        year=disp_year,
      )
    ]
    # populate the player drop down
    self.pair_drop_down.items = [
      (row["pair"], row)
      for row in app_tables.master_pair.search(
        tables.order_by("pair"),
        league=disp_league,
        gender=disp_gender,
        year=disp_year,
      )
    ]
    # set the sstart and end date to beginning and end of the season (needed for function call)
    # self.start_date_picker.date = datetime.date.today()  # temp : need to pull from league DB
    # self.end_date_picker.date = datetime.date.today()

    pass

  def generate_report_button_click(self, **event_args):
    """This method is called when the button is clicked"""

    # unpack the source data:
    user_row = anvil.users.get_user(allow_remembered=True)
    disp_team = user_row["team"]

    # unpack the report to process
    # replace this with a data driven approach
    rpt_name = self.report_drop_down.selected_value
    function_list = [
      (f_row["function_name"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    form_list = [
      (f_row["rpt_form"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    fnct_name = function_list[0]
    rpt_form = form_list[0]

    # builld the rpt_filter 
    rpt_filters = {}

    #pair, player, opp pair
    if self.rpt_detail.selected:
      rpt_filters['rpt_detail'] = 'detail'
    if self.rpt_summary.selected:
      rpt_filters['rpt_detail'] = 'summary'


    if self.opp_pair_drop_down.selected_value:
      rpt_filters['opp_pair'] == self.opp_pair_drop_down.selected_value
      
    rpt_filters['pair'] = self.pair_drop_down.selected_value['pair']
    rpt_filters['player'] = self.player_drop_down.selected_value['team'] + " "+self.player_drop_down.selected_value['number']+' '+self.player_drop_down.selected_value['shortname']
    
    if self.comp_l1_check_box.checked:
      rpt_filters['comp_l1'] = self.comp_l1_drop_down.selected_value['comp_l1']
    if self.comp_l2_check_box.checked:
      rpt_filters['comp_l2'] = self.comp_l2_drop_down.selected_value['comp_l2']
    if self.comp_l3_check_box.checked:
      rpt_filters['comp_l3'] = self.comp_l3_drop_down.selected_value['comp_l3']
    if self.date_check_box.checked:
      rpt_filters['start_date'] = self.start_date_picker.date
      rpt_filters['end_date'] = self.end_date_picker.date
      
    if self.set_1.selected:
      rpt_filters['set'] = 1
    if self.set_2.selected:
      rpt_filters['set'] = 2
    if self.set_3.selected:
      rpt_filters['set'] = 3

    if self.set_bump.selected:
      rpt_filters['set_touch_type'] = 'bump'
    if self.set_hand.selected:
      rpt_filters['set_touch_type'] = 'hand'
    if self.set_unknown.selected:
      rpt_filters['set_touch_type'] = 'unknown'

    if self.pass_insys.selected:
      rpt_filters['pass_oos'] = 0
    if self.pass_oos.selected:
      rpt_filters['pass_oos'] = 1
    
    if self.att_ht_low.text:
      rpt_filters['att_ht_low'] = self.att_ht_low.text
    if self.att_ht_high.text:
      rpt_filters['att_ht_high'] = self.att_ht_high.text
    if self.set_ht_low.text:
      rpt_filters['set_ht_low'] = self.set_ht_low.text
    if self.set_ht_high.text:
      rpt_filters['set_ht_high'] = self.set_ht_high.text
    if self.att_speed_low.text:
        rpt_filters['att_speed_low'] = self.att_speed_low.text
    if self.att_speed_high.text:
      rpt_filters['att_speed_high'] = self.att_speed_high.text
    if self.pass_ht_low.text:
      rpt_filters['pass_ht_low'] = self.pass_ht_low.text
    if self.pass_ht_high.text:
      rpt_filters['pass_ht_high'] = self.pass_ht_high.text

    # now, time for srv_fr and srv_to
    srv_fr = []
    if self.srv_fr_1.checked:
      srv_fr.append('1')
    if self.srv_fr_3.checked:
      srv_fr.append('3')
    if self.srv_fr_5.checked:
      srv_fr.append('5')
    if len(srv_fr) != 0:
      rpt_filters['srv_fr'] = srv_fr
      
    srv_to = []
    if self.check_box_1c.checked:
      srv_to.append('1C')
    if self.check_box_1d.checked:
      srv_to.append('1D')
    if self.check_box_1e.checked:
      srv_to.append('1E')
    if self.check_box_2c.checked:
      srv_to.append('2C')
    if self.check_box_2d.checked:
      srv_to.append('2D')
    if self.check_box_2e.checked:
      srv_to.append('2E')
    if self.check_box_3c.checked:
      srv_to.append('3C')
    if self.check_box_3d.checked:
      srv_to.append('3D')
    if self.check_box_3e.checked:
      srv_to.append('3E')
    if self.check_box_4c.checked:
      srv_to.append('4C')
    if self.check_box_4d.checked:
      srv_to.append('4D')
    if self.check_box_4e.checked:
      srv_to.append('4E')
    if self.check_box_5c.checked:
      srv_to.append('5C')
    if self.check_box_5d.checked:
      srv_to.append('5D')
    if self.check_box_5e.checked:
      srv_to.append('5E')
    if len(srv_to) != 0:
      rpt_filters['srv_to'] = srv_to


      
    #print(f"Report Filters: {rpt_filters}")

    # ---------------------------------------------------------------------------------
    # Generate report and get report ID
    # call the server function to run the report function and store the data in the table.
    report_id = anvil.server.call('generate_and_store_report', fnct_name, self.league_drop_down.selected_value, disp_team, **rpt_filters)
    app_url = 'https://beachinternals.anvil.app/'  # Replace with your app URL
    new_window = anvil.js.window.open(f'{app_url}?form={rpt_form}&report_id={report_id}', '_blank')
    #--------------------------------------------------------------------
    

    pass

  
  def end_date_picker_change(self, **event_args):
    """This method is called when the selected date changes"""
    pass

  def start_date_picker_change(self, **event_args):
    """This method is called when the selected date changes"""
    pass

  def report_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    pass

  def player_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    pass

  def pdf_button_click(self, **event_args):
    """This method is called when the button is clicked"""

    # unpack the source data:
    user_row = anvil.users.get_user(allow_remembered=True)
    disp_team = user_row["team"]

    # unpack the report to process
    # replace this with a data driven approach
    rpt_name = self.report_drop_down.selected_value
    function_list = [
      (f_row["function_name"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    form_list = [
      (f_row["rpt_form"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    fnct_name = function_list[0]
    rpt_form = form_list[0]

    # builld the rpt_filter 
    rpt_filters = {}

    #pair, player, opp pair
    rpt_filters['pair'] = self.pair_drop_down.selected_value['pair']
    #rpt_filters['player'] = self.player_drop_down.selected_value['player']

    if self.opp_pair_drop_down.selected_value:
      rpt_filters['opp_pair'] == self.opp_pair_drop_down.selected_value

    rpt_filters['pair'] = self.pair_drop_down.selected_value['pair']
    rpt_filters['player'] = self.player_drop_down.selected_value['team'] + " "+self.player_drop_down.selected_value['number']+' '+self.player_drop_down.selected_value['shortname']

    if self.comp_l1_check_box.checked:
      rpt_filters['comp_l1'] = self.comp_l1_drop_down.selected_value['comp_l1']
    if self.comp_l2_check_box.checked:
      rpt_filters['comp_l2'] = self.comp_l2_drop_down.selected_value['comp_l2']
    if self.comp_l3_check_box.checked:
      rpt_filters['comp_l3'] = self.comp_l3_drop_down.selected_value['comp_l3']
    if self.date_check_box.checked:
      rpt_filters['start_date'] = self.start_date_picker.date
      rpt_filters['end_date'] = self.end_date_picker.date

    if self.set_1.selected:
      rpt_filters['set'] = 1
    if self.set_2.selected:
      rpt_filters['set'] = 2
    if self.set_3.selected:
      rpt_filters['set'] = 3

    if self.set_bump.selected:
      rpt_filters['set_touch_type'] = 'bump'
    if self.set_hand.selected:
      rpt_filters['set_touch_type'] = 'hand'
    if self.set_unknown.selected:
      rpt_filters['set_touch_type'] = 'unknown'

    if self.pass_insys.selected:
      rpt_filters['pass_oos'] = 0
    if self.pass_oos.selected:
      rpt_filters['pass_oos'] = 1

    if self.att_ht_low.text:
      rpt_filters['att_ht_low'] = self.att_ht_low.text
    if self.att_ht_high.text:
      rpt_filters['att_ht_high'] = self.att_ht_high.text
    if self.set_ht_low.text:
      rpt_filters['set_ht_low'] = self.set_ht_low.text
    if self.set_ht_high.text:
      rpt_filters['set_ht_high'] = self.set_ht_high.text
    if self.att_speed_low.text:
      rpt_filters['att_speed_low'] = self.att_speed_low.text
    if self.att_speed_high.text:
      rpt_filters['att_speed_high'] = self.att_speed_high.text
    if self.pass_ht_low.text:
      rpt_filters['pass_ht_low'] = self.pass_ht_low.text
    if self.pass_ht_high.text:
      rpt_filters['pass_ht_high'] = self.pass_ht_high.text

    # now, time for srv_fr and srv_to
    srv_fr = []
    if self.srv_fr_1.checked:
      srv_fr.append('1')
    if self.srv_fr_3.checked:
      srv_fr.append('3')
    if self.srv_fr_5.checked:
      srv_fr.append('5')
    if len(srv_fr) != 0:
      rpt_filters['srv_fr'] = srv_fr

    srv_to = []
    if self.check_box_1c.checked:
      srv_to.append('1C')
    if self.check_box_1d.checked:
      srv_to.append('1D')
    if self.check_box_1e.checked:
      srv_to.append('1E')
    if self.check_box_2c.checked:
      srv_to.append('2C')
    if self.check_box_2d.checked:
      srv_to.append('2D')
    if self.check_box_2e.checked:
      srv_to.append('2E')
    if self.check_box_3c.checked:
      srv_to.append('3C')
    if self.check_box_3d.checked:
      srv_to.append('3D')
    if self.check_box_3e.checked:
      srv_to.append('3E')
    if self.check_box_4c.checked:
      srv_to.append('4C')
    if self.check_box_4d.checked:
      srv_to.append('4D')
    if self.check_box_4e.checked:
      srv_to.append('4E')
    if self.check_box_5c.checked:
      srv_to.append('5C')
    if self.check_box_5d.checked:
      srv_to.append('5D')
    if self.check_box_5e.checked:
      srv_to.append('5E')
    if len(srv_to) != 0:
      rpt_filters['srv_to'] = srv_to

    #print(f"Report Filters: {rpt_filters}")

    # ---------------------------------------------------------------------------------
    # Generate report and get report ID
    # call the server function to run the report function and store the data in the table.
    report_id = anvil.server.call('generate_and_store_report', fnct_name, self.league_drop_down.selected_value, disp_team, **rpt_filters)
    #app_url = 'https://beachinternals.anvil.app/'  # Replace with your app URL
    #new_window = anvil.js.window.open(f'{app_url}?form={rpt_form}&report_id={report_id}', '_blank')
    #--------------------------------------------------------------------

    pdf_rpt = anvil.server.call("generate_pdf_report", rpt_form, report_id)
      
    result = anvil.server.call(
      "send_email",
      "Beach Internals Player Report - PDF Version",
      "Attached please find the PDF version of the Report",
      pdf_rpt,
      "",
      "",
    )

    #alert(("PDF report emailed" + str(result)))
    anvil.media.download(pdf_rpt)
    
    pass

  def all_rpt_pdf_click(self, **event_args):
    """This method is called when the button is clicked"""
    # take the given (league+) player, and run thru the list and generate all reports, email to user, and download here

    # unpck the league, gender, and year
    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index("|")
    disp_league = league_value[: str_loc - 1].strip()
    league_value = league_value[str_loc + 1 :]
    str_loc = league_value.index("|")
    disp_gender = league_value[: str_loc - 1].strip()
    disp_year = league_value[str_loc + 1 :].strip()

    # unpack the player
    disp_player = (
      self.player_drop_down.selected_value["team"]
      + " "
      + self.player_drop_down.selected_value["number"]
      + " "
      + self.player_drop_down.selected_value["shortname"]
    )

    # unpack the source data:
    user_row = anvil.users.get_user(allow_remembered=True)
    disp_team = user_row["team"]
    user_email = user_row["email"]

    # unpack the report to process
    # replace this with a data driven approach
    rpt_name = self.report_drop_down.selected_value
    function_list = [
      (f_row["function_name"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    text_list = [
      (f_row["explain_text"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    # print(function_list)
    fnct_name = function_list[0]
    table_data4 = text_list[0]
    scout = True

    # now, call the server module.
    # now including limits on competition (1,2,3) and dates
    # check comp_l3, if not, set to str()
    if type(self.comp_l3_drop_down.selected_value["comp_l3"]) == type(None):
      self.comp_l3_drop_down.selected_value["comp_l3"] = str()

    disp_pair = ""  # this is a dummy for player reports to keep the calling arguments consistent for player and pair reports
    return_value = anvil.server.call(
      "render_all_rpts_pdf_callable",
      disp_league,
      disp_gender,
      disp_year,
      disp_team,
      disp_pair,
      disp_player,
      self.comp_l1_check_box.checked,
      self.comp_l1_drop_down.selected_value["comp_l1"],
      self.comp_l2_check_box.checked,
      self.comp_l2_drop_down.selected_value["comp_l2"],
      self.comp_l3_check_box.checked,
      self.comp_l3_drop_down.selected_value["comp_l3"],
      self.date_check_box.checked,
      self.start_date_picker.date,
      self.end_date_picker.date,
      scout,
      table_data4,
      "player",
      user_email,
    )

    alert("PDF report running in background, PDF will be emailed")

    pass

  def summary_pdf_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    # take the given (league+) player, and run thru the list and generate all reports, email to user, and download here

    # unpck the league, gender, and year
    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index("|")
    disp_league = league_value[: str_loc - 1].strip()
    league_value = league_value[str_loc + 1 :]
    str_loc = league_value.index("|")
    disp_gender = league_value[: str_loc - 1].strip()
    disp_year = league_value[str_loc + 1 :].strip()

    # unpack the player
    disp_player = (
      self.player_drop_down.selected_value["team"]
      + " "
      + self.player_drop_down.selected_value["number"]
      + " "
      + self.player_drop_down.selected_value["shortname"]
    )

    # unpack the source data:
    user_row = anvil.users.get_user(allow_remembered=True)
    disp_team = user_row["team"]

    # unpack the report to process
    # replace this with a data driven approach
    rpt_name = self.report_drop_down.selected_value
    function_list = [
      (f_row["function_name"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    text_list = [
      (f_row["explain_text"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    form_list = [
      (f_row["rpt_form"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    # print(function_list)
    fnct_name = function_list[0]  # name of function to call
    table_data4 = text_list[0]  # explain text
    rpt_form = form_list[0]  # name of form for PDF render
    scout = True

    # now, call the server module.
    # now including limits on competition (1,2,3) and dates
    # check comp_l3, if not, set to str()
    if type(self.comp_l3_drop_down.selected_value["comp_l3"]) == type(None):
      self.comp_l3_drop_down.selected_value["comp_l3"] = str()

    pdf_rpt = anvil.server.call(
      "create_pdf_reports",
      fnct_name,
      rpt_form,
      disp_league,
      disp_gender,
      disp_year,
      disp_team,
      disp_player,
      self.comp_l1_check_box.checked,
      self.comp_l1_drop_down.selected_value["comp_l1"],
      self.comp_l2_check_box.checked,
      self.comp_l2_drop_down.selected_value["comp_l2"],
      self.comp_l3_check_box.checked,
      self.comp_l3_drop_down.selected_value["comp_l3"],
      self.date_check_box.checked,
      self.start_date_picker.date,
      self.end_date_picker.date,
      scout,
      table_data4,
    )

    result = anvil.server.call(
      "send_email",
      "Beach Internals Player Summary - PDF Version",
      "Attached please find the PDF version of the Player Summary",
      pdf_rpt,
      "",
      "",
    )
    alert(("PDF report emailed" + str(result)))
    anvil.media.download(pdf_rpt)
    pass

  def button_1_click(self, **event_args):
    """This method is called when the button is clicked"""
    print(anvil.server.call('test_now'))
    pass

  def outlined_button_1_click(self, **event_args):
    """This method is called when the button is clicked"""
    # unpack the league data:
    # extract league, gender, year from league selected value

    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index("|")
    disp_league = league_value[: str_loc - 1].strip()
    league_value = league_value[str_loc + 1 :]
    str_loc = league_value.index("|")
    disp_gender = league_value[: str_loc - 1].strip()
    disp_year = league_value[str_loc + 1 :].strip()

    # unpack the player
    disp_player = (
      self.player_drop_down.selected_value["team"]
      + " "
      + self.player_drop_down.selected_value["number"]
      + " "
      + self.player_drop_down.selected_value["shortname"]
    )

    # unpack the source data:
    user_row = anvil.users.get_user(allow_remembered=True)
    disp_team = user_row["team"]

    # unpack the report to process
    # replace this with a data driven approach
    rpt_name = self.report_drop_down.selected_value
    function_list = [
      (f_row["function_name"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    form_list = [
      (f_row["rpt_form"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    fnct_name = function_list[0]
    rpt_form = form_list[0]

    # ---------------------------------------------------------------------------------
    # Generate report and get report ID
    # call the server function to run the report function and store the data in the table.
    report_id = anvil.server.call('generate_and_store_report', fnct_name)
    label_list, image_list, df_list = anvil.server.call('get_report_data', report_id)

    print(f" In Reports-Main: Label List: {label_list}, Image list {image_list}, df list {df_list}")
    pass

  def rpt_type_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    # populate the reports drop down
    if anvil.users.get_user()["team"] == "INTERNALS":
      self.report_drop_down.items = [
        (row["report_name"]) for row in app_tables.report_list.search(rpt_type=self.rpt_type_drop_down.selected_value)
      ]
    else:
      self.report_drop_down.items = [
        (row["report_name"])
        for row in app_tables.report_list.search(private=False, rpt_type=self.rpt_type_drop_down.selected_value)
      ]
    pass
