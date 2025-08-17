from ._anvil_designer import player_table_report_newTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class player_table_report_new(player_table_report_newTemplate):
  def __init__(self, report_id=None, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.
    # self.label_1.text = display_data or "No data provided"

    # Create a header panel
    header_panel = LinearPanel()
    header_panel.background = 'gray'
    header_label = Label(text='Beach Internals, scott@beachinternals.com', foreground='black', font_size=16)
    header_panel.add_component(header_label)
    self.add_component(header_panel)

    # ------------------------------
    if report_id:
      # Fetch report data from server
      title_list, label_list, image_list, df_list, df_desc_list, image_desc_list = anvil.server.call(
        "get_report_data", report_id
      )

      """
      Labels and titles are stored as below:
        title_list[0] = rpt_row['rpt_title']
        title_list[1] = rpt_row['rpt_sub_title']
        title_list[2] = rpt_row['rpt_section_title1']
        title_list[3] = rpt_row['lgy']
        title_list[4] = rpt_row['team_name']
        title_list[5] = rpt_row['rpt_type']
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

      # populate the titles
      if isinstance(title_list[0], str):
        self.rpt_title.text = title_list[0]
      if isinstance(title_list[1],str) and isinstance(title_list[8], str):
        self.rpt_subtitle.text = title_list[1] + title_list[8]
      if isinstance(title_list[7], str):
        self.explain_text.content = title_list[7]
      if isinstance(title_list[8], str):
        self.filter_text.content = title_list[6]

      #------------------------------------------------
      # populate the box1 label and table
      if isinstance(label_list[0], str):
        self.box1_title.text = label_list[0]
      if isinstance(df_list[0], str):
        if len(df_list[0]) != 0:
          self.box1.content = df_list[0]
      elif isinstance(df_list[0], anvil.Media):
        if (len(df_list[0].get_bytes()) != 0):
          self.box1.content = df_list[0]

      #------------------------------------------------
      # populate the box2 label and table
      if isinstance(label_list[1], str):
        self.box2_title.text = label_list[1]
      if isinstance(df_list[1], str):
        if len(df_list[1]) != 0:
          self.box2.content = df_list[1]
      elif isinstance(df_list[1], anvil.Media):
        if (len(df_list[1].get_bytes()) != 0):
          self.box2.content = df_list[1]

      #------------------------------------------------
      # populate the box3 label and table
      if isinstance(label_list[2], str):
        self.box3_title.text = label_list[2]
      if isinstance(df_list[2], str):
        if len(df_list[2]) != 0:
          self.box3.content = df_list[2]
      elif isinstance(df_list[2], anvil.Media):
        if (len(df_list[2].get_bytes()) != 0):
          self.box3.content = df_list[2]

      #------------------------------------------------
      # populate the box4 label and table
      if isinstance(label_list[3], str):
        self.box4_title.text = label_list[3]
      if isinstance(df_list[3], str):
        if len(df_list[3]) != 0:
          self.box4.content = df_list[3]
      elif isinstance(df_list[3], anvil.Media):
        if (len(df_list[3].get_bytes()) != 0):
          self.box4.content = df_list[3]

    else:
      self.label_1.text = "No Report Data Found"
