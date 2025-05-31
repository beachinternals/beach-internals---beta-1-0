from ._anvil_designer import popupformTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class popupform(popupformTemplate):
  def __init__(self, report_id=None, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.
    #self.label_1.text = display_data or "No data provided"

    #------------------------------
    if report_id:
      # Fetch report data from server
      title_list, label_list, image_list, df_list = anvil.server.call('get_report_data', report_id)

      '''
      Labels and titles are stored as below: 
        title_list[0] = rpt_row['rpt_title']
        title_list[1] = rpt_row['rpt_sub_title']
        title_list[2] = rpt_row['rpt_section_title1']
        title_list[3] = rpt_row['rpt_section_title2']
        title_list[4] = rpt_row['team_name']
        title_list[5] = rpt_row['rpt_type']
        title_list[6] = rpt_row['filter_text']
        title_list[7] = rpt_row['explain_text']
  
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
      '''


      self.title.text = title_list[0]

      for i in range(0,len(label_list)):
        if label_list[i]:
          if i == 0:
            self.label_1.text = label_list[i]
          elif i == 1:
            self.label_2.text = label_list[i]
          elif i == 2:
            self.label_3.text = label_list[i]
          elif i == 3:
            self.label_4.text = label_list[i]
            
    else:
      self.label_1.text = 'No Report Data Found'