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
      label_list, image_list, df_list = anvil.server.call('get_report_data', report_id)

      self.title.text = 'Test Report'

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