from ._anvil_designer import matchup_serves_rptTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class matchup_serves_rpt(matchup_serves_rptTemplate):
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

    # find by four players frm teh two pairs
    player_a1, player_a2 = anvil.server.call( 'pair_players', disp_pair_a )
    player_b1, player_b2 = anvil.server.call( 'pair_players', disp_pair_b )
    
    self.pair_a_label.text = disp_pair_a
    self.pair_b_label.text = disp_pair_b
    self.serve_strategies_table.content = table_data1
    self.plt1.source = plt1
    self.plt2.source = plt2
    self.plt3.source = plt3
    self.plt4.source = plt4
    self.plt5.source = plt5
    self.plt6.source = plt6
    self.plt1_label.text = box_title1 + player_b1
    self.plt2_label.text = box_title2 + player_b2
    self.plt3_label.text = box_title3 + player_b1
    self.plt4_label.text = box_title4 + player_b2
    self.plt5_label.text = box_title5 + player_b1
    self.plt6_label.text = box_title6 + player_b2


    #self.filter_text.content = filter_text
    #self.explain_text.content = explain_text
    # self.box1_label.text = title_text

    # Any code you write here will run before the form opens.
