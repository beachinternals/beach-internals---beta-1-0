from ._anvil_designer import scout_3court_plot_rptTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class scout_3court_plot_rpt(scout_3court_plot_rptTemplate):
  def __init__(
    self,
    disp_pair,
    disp_player,
    srv_strat_title,
    table_data,  
    plot1_title,
    plot1,
    plot2_title,
    plot2,
    plot3_title,
    plot3,
    z1_att,
    z2_att,
    z3_att,
    z4_att,
    z5_att,
    opt_att,
    z1_tbl,
    z2_tbl,
    z3_tbl,
    z4_tbl,
    z5_tbl,
    opt_tbl,
    filter_text,
    explain_text,
    title_text,
    **properties,
  ):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    self.rpt_title.content = title_text
    self.player_label.text = disp_player
    self.pair_label.text = disp_pair
    self.plot1.source = plot1
    self.plt1_title.text = 'From Zone 1'
    self.plt2_title.text = 'From Zone 3'
    self.plt3_title.text = 'From Zone 5'
    self.plot2.source = plot2
    self.plot3.source = plot3
    self.filter_text.content = filter_text
    self.explain_text.content = explain_text


    # Any code you write here will run before the form opens.
