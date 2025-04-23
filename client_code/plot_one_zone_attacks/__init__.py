from ._anvil_designer import plot_one_zone_attacksTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class plot_one_zone_attacks(plot_one_zone_attacksTemplate):
  def __init__(
    self,
    disp_pair,
    disp_player,
    serve_strategy_title,
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

    #self.srv_strategy_title.content = serve_strategy_title
    self.pair_player_label.text = 'Player:'
    self.pair_player_text.text = disp_player
    self.rpt_disp_box.content = table_data
    self.serve_plot.source = plot1
    self.pass_plot.source = plot2
    self.set_plot.source = plot3
    self.z1_att_plot.source = z1_att
    #self.z2_att_plot.source = z2_att
    #self.z3_att_plot.source = z3_att
    #self.z4_att_plot.source = z4_att
    #self.z5_att_plot.source = z5_att
    #self.opt_att_plot.source = opt_att
    self.z1_table.content = z1_tbl
    #self.z2_table.content = z2_tbl
    #self.z3_table.content = z3_tbl
    #self.z4_table.content = z4_tbl
    #self.z5_table.content = z5_tbl
    #self.opt_table.content = opt_tbl
    #self.pair_label.text = disp_pair
    self.filter_text.content = filter_text
    self.explain_text.content = explain_text
    # self.box1_label.text = title_text
    self.rpt_title.text = serve_strategy_title
    self.title2.text = serve_strategy_title
    self.player_label2.text = disp_player
    

    # Any code you write here will run before the form opens.
