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
    rpt_title,
    disp_pair,
    disp_player2,
    table_data1,
    table_data2,
    table_data3,
    table_data4,
    table_data5,
    table_data6,
    table_data7,
    table_data8,
    box1_title, 
    box2_title, 
    box3_title,
    box4_title, 
    box5_title,
    box6_title, 
    box7_title, 
    box8_title,
    filter_text,
    explain_text,
    **properties,
  ):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    self.pair_label.text = disp_pair
    self.rpt_disp_box1.content = table_data1
    self.rpt_disp_box2.content = table_data2
    self.rpt_disp_box3.content = table_data3
    self.explain_text.content = explain_text
    self.filter_text.content = filter_text

    # the init arguments are set up as the standard set of argument for a pair report (most with 8 blocks).  THis only has 3 blocks of data, so the other calling argumanet end up a dummies
    # Any code you write here will run before the form opens.
