from ._anvil_designer import pair_summary_rptTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class pair_summary_rpt(pair_summary_rptTemplate):
  def __init__(
    self,
    table_data1,
    table_data2,
    table_data3,
    filter_text,
    explain_text,
    disp_player,
    report_title, 
    box1_title, 
    box2_title, 
    box3_title,
    **properties,
  ):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    self.pair_label.text = disp_player
    self.rpt_disp_box.content = table_data1
    self.rpt_disp_box2.content = table_data2
    self.rpt_disp_box3.content = table_data3
    self.rpt_disp_box4.content = explain_text
    self.filter_text.content = filter_text

    # Any code you write here will run before the form opens.
