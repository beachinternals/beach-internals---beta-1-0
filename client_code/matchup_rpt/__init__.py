from ._anvil_designer import matchup_rptTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class matchup_rpt(matchup_rptTemplate):
  def __init__(
    self,
    disp_pair,
    disp_pair_a,
    disp_pair_b,
    table_data1, 
    table_data2, 
    table_data3, 
    table_data4, 
    table_data5, 
    table_data6, 
    table_data7, 
    table_data8, 
    plt1, 
    plt2, 
    plt3, 
    plt4, 
    plt5, 
    plt6,
    box_title1,
    box_title2,
    box_title3,
    box_title4,
    box_title5,
    box_title6,
    box_title7,
    box_title8,
    filter_text,
    explain_text,
    **properties,
  ):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)
    
    self.pair_a_label.text = disp_pair_a
    self.pair_b_label.text = disp_pair_b
    self.srva_perference.content = table_data2
    self.srvb_preferece.content = table_data3
    self.matchup_prediction_mkdn.content = table_data1
    #self.filter_text.content = filter_text
    #self.explain_text.content = explain_text


    # Any code you write here will run before the form opens.
