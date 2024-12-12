from ._anvil_designer import function_rptTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class function_rpt(function_rptTemplate):
  def __init__(self, table_data1, table_data2, table_data3, filter_text, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    self.rpt_disp_box.content = table_data1
    self.rpt_disp_box2.content = table_data2
    self.rpt_disp_box3.content = table_data3
    self.rpt_disp_box4 = filter_text

    # Any code you write here will run before the form opens.
