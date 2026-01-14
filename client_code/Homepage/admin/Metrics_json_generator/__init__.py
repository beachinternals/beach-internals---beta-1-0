from ._anvil_designer import Metrics_json_generatorTemplate
import anvil
from anvil.js import window
import anvil.js
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
from anvil.tables import app_tables
import datetime
from anvil import open_form
import anvil.js
import anvil.media


class Metrics_json_generator(Metrics_json_generatorTemplate):
  """
  Form for generating comprehensive player metrics JSON files.
  This is similar to Reports_main but generates JSON instead of reports.
  """

  def __init__(self, **properties):
    # Set Form properties and Data Bindings
    self.init_components(**properties)

    # Check for login
    user_row = anvil.users.get_user(allow_remembered=True)
    if not user_row:
      open_form("Homepage.UserMgr")
    elif not user_row["team"]:
      open_form("Homepage.Contact")

    disp_league = user_row["def_league"]

    # Populate league dropdown
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

    # Populate competition level dropdowns
    self.comp_l1_drop_down.items = [
      (row["comp_l1"], row)
      for row in app_tables.league_comp_l1.search(league=disp_league)
    ]
    self.comp_l2_drop_down.items = [
      (row["comp_l2"], row)
      for row in app_tables.league_comp_l2.search(league=disp_league)
    ]

    # Set comp_l3 data
    comp3lbl = [
      (r["comp_l3_label"], r)
      for r in app_tables.league_list.search(league=user_row["def_league"])
    ]
    self.comp_l3_drop_down.items = [
      (row["comp_l3"], row)
      for row in app_tables.league_comp_l3.search(comp_l3_label=comp3lbl[0][0])
    ]

    # Extract league, gender, year from league selected value
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index("|")
    disp_league = league_value[: str_loc - 1].strip()
    league_value = league_value[str_loc + 1 :]
    str_loc = league_value.index("|")
    disp_gender = league_value[: str_loc - 1].strip()
    disp_year = league_value[str_loc + 1 :].strip()

    # Populate player dropdown
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

  def comp_l1_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    # Extract league from league dropdown
    league_value = self.league_drop_down.selected_value
    str_loc = league_value.index("|")
    disp_league = league_value[: str_loc - 1].strip()

    # Update comp_l2 based on selected comp_l1
    self.comp_l2_drop_down.items = [
      (row["comp_l2"], row)
      for row in app_tables.league_comp_l2.search(
        league=disp_league, comp_l1=self.comp_l1_drop_down.selected_value["comp_l1"]
      )
    ]

  def json_button_click(self, **event_args):
    """This method is called when the Generate JSON button is clicked"""

    # Get user info
    user_row = anvil.users.get_user(allow_remembered=True)
    disp_team = user_row["team"]

    # Build the filter dictionary
    json_filters = {}

    # Player is REQUIRED for JSON generation
    if not self.player_drop_down.selected_value:
      anvil.alert("Please select a player")
      return

    json_filters['player'] = (
      self.player_drop_down.selected_value['team'] + " " +
      self.player_drop_down.selected_value['number'] + ' ' +
      self.player_drop_down.selected_value['shortname']
    )
    json_filters['player_shortname'] = self.player_drop_down.selected_value['shortname']

    # Optional filters
    if self.comp_l1_check_box.checked and self.comp_l1_drop_down.selected_value:
      json_filters['comp_l1'] = self.comp_l1_drop_down.selected_value['comp_l1']

    if self.comp_l2_check_box.checked and self.comp_l2_drop_down.selected_value:
      json_filters['comp_l2'] = self.comp_l2_drop_down.selected_value['comp_l2']

    if self.comp_l3_check_box.checked and self.comp_l3_drop_down.selected_value:
      json_filters['comp_l3'] = self.comp_l3_drop_down.selected_value['comp_l3']

    if self.date_check_box.checked:
      json_filters['start_date'] = self.start_date_picker.date
      json_filters['end_date'] = self.end_date_picker.date

    if self.set_1.selected:
      json_filters['set'] = 1
    elif self.set_2.selected:
      json_filters['set'] = 2
    elif self.set_3.selected:
      json_filters['set'] = 3

    if self.set_bump.selected:
      json_filters['set_touch_type'] = 'bump'
    elif self.set_hand.selected:
      json_filters['set_touch_type'] = 'hand'
    elif self.set_unknown.selected:
      json_filters['set_touch_type'] = 'unknown'

    if self.pass_insys.selected:
      json_filters['pass_oos'] = 0
    elif self.pass_oos.selected:
      json_filters['pass_oos'] = 1

    if self.att_ht_low.text:
      json_filters['att_ht_low'] = self.att_ht_low.text
    if self.att_ht_high.text:
      json_filters['att_ht_high'] = self.att_ht_high.text
    if self.set_ht_low.text:
      json_filters['set_ht_low'] = self.set_ht_low.text
    if self.set_ht_high.text:
      json_filters['set_ht_high'] = self.set_ht_high.text
    if self.att_speed_low.text:
      json_filters['att_speed_low'] = self.att_speed_low.text
    if self.att_speed_high.text:
      json_filters['att_speed_high'] = self.att_speed_high.text
    if self.pass_ht_low.text:
      json_filters['pass_ht_low'] = self.pass_ht_low.text
    if self.pass_ht_high.text:
      json_filters['pass_ht_high'] = self.pass_ht_high.text

    # Serve from zones
    srv_fr = []
    if self.srv_fr_1.checked:
      srv_fr.append('1')
    if self.srv_fr_3.checked:
      srv_fr.append('3')
    if self.srv_fr_5.checked:
      srv_fr.append('5')
    if len(srv_fr) != 0:
      json_filters['srv_fr'] = srv_fr

    # Serve to zones
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
      json_filters['srv_to'] = srv_to

    # Show progress indicator
    self.status_label.text = "Generating metrics JSON... This may take a minute."
    self.status_label.visible = True

    # Call server function to generate JSON
    try:
      result = anvil.server.call(
        'generate_player_metrics_json',
        self.league_drop_down.selected_value,
        disp_team,
        **json_filters
      )

      # result is a dict with 'media_obj' and 'filename'
      if result and 'media_obj' in result:
        # Provide download link
        self.status_label.text = f"✓ JSON generated successfully: {result['filename']}"
        self.download_link.text = "Download JSON File"
        self.download_link.url = result['media_obj']
        self.download_link.visible = True

        # Show summary
        if 'summary' in result:
          summary_text = f"\n\nMetrics calculated: {result['summary'].get('total_metrics', 0)}"
          summary_text += f"\nPoints analyzed: {result['summary'].get('total_points', 0)}"
          self.status_label.text += summary_text

      else:
        self.status_label.text = "Error: Could not generate JSON"

    except Exception as e:
      self.status_label.text = f"Error generating JSON: {str(e)}"
      self.download_link.visible = False

  def player_drop_down_change(self, **event_args):
    """This method is called when an item is selected"""
    # Reset download link when player changes
    self.download_link.visible = False
    self.status_label.text = ""
    pass

  def start_date_picker_change(self, **event_args):
    """This method is called when the selected date changes"""
    pass

  def end_date_picker_change(self, **event_args):
    """This method is called when the selected date changes"""
    pass

  @anvil.handle("upload_dictionary", "click")
  def upload_dictionary_click(self, **event_args):
    """This method is called when the button is clicked"""
    pass

  @anvil.handle("dict_file_loader", "change")
  def dict_file_loader_change(self, **event_args):
    """This method is called when a new file is loaded into this FileLoader"""

    # Get the file from event_args
    csv_file = event_args.get('file')

    if csv_file:
      with anvil.Notification("Importing metrics... this may take a minute"):
        result = anvil.server.call('import_metric_dictionary_from_csv', csv_file)

      if result['success']:
        anvil.alert(f"✓ Successfully imported {result['imported']} metrics!")
        if result['errors']:
          print(f"Errors encountered: {len(result['errors'])}")
          for error in result['errors'][:10]:  # Show first 10 errors
            print(f"  - {error}")
      else:
        anvil.alert(f"Import failed: {result['errors'][0]}")

    pass

