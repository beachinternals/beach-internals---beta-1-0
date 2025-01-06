from ._anvil_designer import PairRptTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import datetime



class PairRpt(PairRptTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)
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

    user_email = user_row['email']

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
      for row in app_tables.league_comp_l1.search(league=user_row["def_league"])
    ]
    self.comp_l2_drop_down.items = [
      (row["comp_l2"], row)
      for row in app_tables.league_comp_l2.search(league=user_row["def_league"])
    ]
    # self.comp_l3_drop_down.items = [(row["comp_l3"], row) for row in app_tables.league_comp_l3.search( comp_l3_label = user_row['def_league'])]

    # set comp_l3 data:
    comp3lbl = [
      (r["comp_l3_label"], r)
      for r in app_tables.league_list.search(league=user_row["def_league"])
    ]
    self.comp_l3_drop_down.items = [
      (row["comp_l3"], row)
      for row in app_tables.league_comp_l3.search(comp_l3_label=comp3lbl[0][0])
    ]

    # before populating the pair table, re-unpack the league drop down vallue
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index("|")
    disp_league = league_value[: str_loc - 1].strip()
    league_value = league_value[str_loc + 1 :]
    str_loc = league_value.index("|")
    disp_gender = league_value[: str_loc - 1].strip()
    disp_year = league_value[str_loc + 1 :].strip()
    
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

    # populate the reports drop down
    if anvil.users.get_user()["team"] == "INTERNALS":
      self.report_drop_down.items = [
        (row["report_name"]) for row in app_tables.report_list.search(rpt_type='pair')
      ]
    else:
      self.report_drop_down.items = [
        (row["report_name"]) for row in app_tables.report_list.search( private=False, rpt_type='pair' )
      ]

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
    # unpack the league data:
    # extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index("|")
    disp_league = league_value[: str_loc - 1].strip()
    league_value = league_value[str_loc + 1 :]
    str_loc = league_value.index("|")
    disp_gender = league_value[: str_loc - 1].strip()
    disp_year = league_value[str_loc + 1 :].strip()
    disp_pair = self.pair_drop_down.selected_value['pair']
    disp_player1, disp_player2 = anvil.server.call('pair_players',disp_pair)
    
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
    box1_title_list = [
      (f_row["box1_title"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    box2_title_list = [
      (f_row["box2_title"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    box3_title_list = [
      (f_row["box3_title"])
      for f_row in app_tables.report_list.search(report_name=rpt_name)
    ]
    # print(function_list)
    fnct_name = function_list[0]
    explain_text = text_list[0]
    box1_title = box1_title_list[0]
    box2_title = box2_title_list[0]
    box3_title = box3_title_list[0]
    scout = True

    # now, call the server module.
    # now including limits on competition (1,2,3) and dates
    # check comp_l3, if not, set to str()
    if type(self.comp_l3_drop_down.selected_value["comp_l3"]) == type(None):
      self.comp_l3_drop_down.selected_value["comp_l3"] = str()

    # call the server function
    table_data1, table_data2, table_data3 = anvil.server.call(
      fnct_name,
      disp_league,
      disp_gender,
      disp_year,
      disp_team,
      self.pair_drop_down.selected_value['pair'],
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
      explain_text,
    )

    # now put this into the rtf box
    filter_text = f"""
    Data Filters:
    - League : {disp_league}
    - Gender : {disp_gender}
    - Year : {disp_year}
    - Pair : {self.pair_drop_down.selected_value['pair']}
    - Competition 1 : {self.comp_l1_drop_down.selected_value['comp_l1'] if self.comp_l1_check_box.checked else ''}
    - Competition 2 : {self.comp_l2_drop_down.selected_value['comp_l2'] if self.comp_l2_check_box.checked else ''}
    - Competition 3 : {self.comp_l3_drop_down.selected_value['comp_l3'] if self.comp_l3_check_box.checked else ''}
    - Date Filtered : {str(self.start_date_picker.date)+' to '+str(self.end_date_picker.date) if self.date_check_box.checked else ''}
    """

    self.rich_text_2.content = filter_text
    self.rpt_disp_box.content = table_data1
    self.rpt_disp_box2.content = table_data2
    self.rpt_disp_box3.content = table_data3
    self.rpt_disp_box4.content = explain_text
    self.box1_label.text = box1_title
    self.box2_label.text = box2_title + ' ' + disp_player1
    self.box3_label.text = box3_title + ' ' + disp_player2

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

  def pair_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    pass

  def pdf_button_click(self, **event_args):
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
    disp_pair = self.pair_drop_down.selected_value['pair']

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
    fnct_name = function_list[0]
    table_data4 = text_list[0]
    form = form_list[0]
    scout = True

    # now, call the server module.
    # now including limits on competition (1,2,3) and dates
    # check comp_l3, if not, set to str()
    if type(self.comp_l3_drop_down.selected_value["comp_l3"]) == type(None):
      self.comp_l3_drop_down.selected_value["comp_l3"] = str()

    # call the server function
    pdf_rpt = anvil.server.call(
      "create_pdf_reports",
      fnct_name,
      form,
      disp_league,
      disp_gender,
      disp_year,
      disp_team,
      disp_pair,
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
      table_data4
    )
    result = anvil.server.call(
      "send_email",
      "Beach Internals Pair Report - PDF Version",
      "Attached please find the PDF version of the Pair Report",
      pdf_rpt,
      "",
      "",
    )
    alert(("PDF report emailed" + str(result)))
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


    # unpack the source data:
    user_row = anvil.users.get_user(allow_remembered=True)
    disp_team = user_row["team"]
    user_email = user_row['email']

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

    return_value = anvil.server.call(
      "render_all_rpts_pdf_callable",
      disp_league,
      disp_gender,
      disp_year,
      disp_team,
      self.pair_drop_down.selected_value['pair'],
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
      'pair',
      user_email
    )

    alert("PDF report running in background")

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
    anvil.media.download(pdf_rpt, filename = 'filename')
    pass


