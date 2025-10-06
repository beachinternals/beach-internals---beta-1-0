from ._anvil_designer import player_correlation_rpt_newTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class player_correlation_rpt_new(player_correlation_rpt_newTemplate):
  def __init__(self, report_id=None, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.
    # self.label_1.text = display_data or "No data provided"

    # Create a header panel
    header_panel = LinearPanel()
    header_panel.background = "gray"
    header_label = Label(
      text="Beach Internals, info@beachinternals.com", foreground="black", font_size=14
    )
    header_panel.add_component(header_label)
    self.add_component(header_panel)

    # ------------------------------
    if report_id:
      # Fetch report data from server
      title_list, label_list, image_list, df_list, df_desc_list, image_desc_list = (
        anvil.server.call("get_report_data", report_id)
      )

      """
      Labels and titles are stored as below:
        title_list[0] = rpt_row['rpt_title']
        title_list[1] = rpt_row['rpt_sub_title']
        title_list[2] = rpt_row['rpt_section_title1']
        title_list[3] = rpt_row['lgy']
        title_list[4] = rpt_row['team_name']
        title_list[5] = rpt_row['report_type'] # pair, player, matchup, dashboard, scouting, internals
        title_list[6] = rpt_row['filter_text']
        title_list[7] = rpt_row['explain_text']
        title_list[8] = rpt_row['player']
        title_list[9] = rpt_row['pair']

        label_list[0] = rpt_row['box1_title']
        label_list[1] = rpt_row['box2_title']
        label_list[2] = rpt_row['box3_title']
        label_list[3] = rpt_row['box4_title']
        label_list[4] = rpt_row['box5_title']
        label_list[5] = rpt_row['box6_title']
        label_list[6] = rpt_row['box7_title']
        label_list[7] = rpt_row['box8_title']
        label_list[8] = rpt_row['box9_title']
        label_list[9] = rpt_row['box10_title']
      """
      # ---------------------------------------
      #
      # Populate The titles
      #
      # ----------------------------------------
      # populate the title
      # Report Title
      # Report Sub-title + player or pair
      if isinstance(title_list[0], str):
        self.rpt_title.text = title_list[0]
      if isinstance(title_list[1], str) and isinstance(title_list[8], str):
        if isinstance(title_list[5], str):
          if title_list[5] == "pair":
            self.rpt_subtitle.text = title_list[1] + title_list[9]  # for a pair report
          elif title_list[5] == "player":
            self.rpt_subtitle.text = (
              title_list[1] + title_list[8]
            )  # for a player report
          elif title_list[5] == "league":
            self.rpt_subtitle.text = (
              title_list[1] + title_list[3]
            )  # for a league report
          else:
            self.rpt_subtitle.text = title_list[1]

      if isinstance(title_list[7], str):
        self.explain_text.content = title_list[7]
      if isinstance(title_list[6], str):
        self.filter_text.content = title_list[6]

      # ---------------------------------------
      #
      # Populate The box titles
      #
      # ----------------------------------------
      if isinstance(label_list[0], str):
        self.box1_title.text = label_list[0]
      if isinstance(label_list[1], str):
        self.box2_title.text = label_list[1]
      if isinstance(label_list[2], str):
        self.box3_title.text = label_list[2]
      if isinstance(label_list[3], str):
        self.box4_title.text = label_list[3]
      if isinstance(label_list[4], str):
        self.box5_title.text = label_list[4]
      # if isinstance(label_list[5], str):
      #  self.box6_title.text = label_list[5]
      """
      if isinstance(label_list[6], str):
        self.box7_title.text = label_list[6]
      if isinstance(label_list[7], str):
        self.box8_title.text = label_list[7]
      if isinstance(label_list[8], str):
        self.box9_title.text = label_list[8]
      if isinstance(label_list[9], str):
        self.box10_title.text = label_list[9]
      """

      # ---------------------------------------
      #
      # Populate The plots / images
      #
      # ----------------------------------------
      if isinstance(image_list[0], str):
        if len(image_list[0]) != 0:
          self.image_1.source = image_list[0]
      elif isinstance(image_list[0], anvil.Media):
        if len(image_list[0].get_bytes()) != 0:
          self.image_1.source = image_list[0]

      if isinstance(image_list[1], str):
        if len(image_list[1]) != 0:
          self.image_2.source = image_list[1]
      elif isinstance(image_list[1], anvil.Media):
        if len(image_list[1].get_bytes()) != 0:
          self.image_2.source = image_list[1]

      if isinstance(image_list[2], str):
        if len(image_list[2]) != 0:
          self.image_3.source = image_list[2]
      elif isinstance(image_list[2], anvil.Media):
        if len(image_list[2].get_bytes()) != 0:
          self.image_3.source = image_list[2]

      if isinstance(image_list[3], str):
        if len(image_list[3]) != 0:
          self.image_4.source = image_list[3]
      elif isinstance(image_list[3], anvil.Media):
        if len(image_list[3].get_bytes()) != 0:
          self.image_4.source = image_list[3]

      if isinstance(image_list[4], str):
        if len(image_list[4]) != 0:
          self.image_5.source = image_list[4]
      elif isinstance(image_list[4], anvil.Media):
        if len(image_list[4].get_bytes()) != 0:
          self.image_5.source = image_list[4]

      if isinstance(image_list[5], str):
        if len(image_list[5]) != 0:
          self.image_6.source = image_list[5]
      elif isinstance(image_list[5], anvil.Media):
        if len(image_list[5].get_bytes()) != 0:
          self.image_6.source = image_list[5]

      if isinstance(image_list[6], str):
        if len(image_list[6]) != 0:
          self.image_7.source = image_list[6]
      elif isinstance(image_list[6], anvil.Media):
        if len(image_list[6].get_bytes()) != 0:
          self.image_7.source = image_list[6]

      if isinstance(image_list[7], str):
        if len(image_list[7]) != 0:
          self.image_8.source = image_list[7]
      elif isinstance(image_list[7], anvil.Media):
        if len(image_list[7].get_bytes()) != 0:
          self.image_8.source = image_list[7]

      if isinstance(image_list[8], str):
        if len(image_list[8]) != 0:
          self.image_9.source = image_list[8]
      elif isinstance(image_list[8], anvil.Media):
        if len(image_list[8].get_bytes()) != 0:
          self.image_9.source = image_list[8]

      if isinstance(image_list[9], str):
        if len(image_list[9]) != 0:
          self.image_10.source = image_list[9]
      elif isinstance(image_list[9], anvil.Media):
        if len(image_list[9].get_bytes()) != 0:
          self.image_10.source = image_list[9]

      # ---------------------------------------
      #
      # Populate The dataframes / rich text / tables
      #
      # ----------------------------------------

      if isinstance(df_list[0], str):
        if len(df_list[0]) != 0:
          self.box_1.content = df_list[0]
      elif isinstance(df_list[0], anvil.Media):
        if len(df_list[0].get_bytes()) != 0:
          self.box_1.content = df_list[0]

      if isinstance(df_list[1], str):
        if len(df_list[1]) != 0:
          self.box_2.content = df_list[1]
      elif isinstance(df_list[1], anvil.Media):
        if len(df_list[1].get_bytes()) != 0:
          self.box_2.content = df_list[1]

      if isinstance(df_list[2], str):
        if len(df_list[2]) != 0:
          self.box_3.content = df_list[2]
      elif isinstance(df_list[2], anvil.Media):
        if len(df_list[2].get_bytes()) != 0:
          self.box_3.content = df_list[2]

      if isinstance(df_list[3], str):
        if len(df_list[3]) != 0:
          self.box_4.content = df_list[3]
      elif isinstance(df_list[3], anvil.Media):
        if len(df_list[3].get_bytes()) != 0:
          self.box_4.content = df_list[3]

      if isinstance(df_list[4], str):
        if len(df_list[4]) != 0:
          self.box_5.content = df_list[4]
      elif isinstance(df_list[4], anvil.Media):
        if len(df_list[4].get_bytes()) != 0:
          self.box_5.content = df_list[4]

      if isinstance(df_list[5], str):
        if len(df_list[5]) != 0:
          self.box_6.content = df_list[5]
      elif isinstance(df_list[5], anvil.Media):
        if len(df_list[5].get_bytes()) != 0:
          self.box_6.content = df_list[5]

    else:
      self.label_1.text = "No Report Data Found"
