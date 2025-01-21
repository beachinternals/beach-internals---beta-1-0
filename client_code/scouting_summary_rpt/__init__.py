from ._anvil_designer import scouting_summary_rptTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class scouting_summary_rpt(scouting_summary_rptTemplate):
  def __init__(
    self,
    serve_strategy_title,
    disp_player,
    table_data,
    srv_plot,
    pass_plot,
    set_plot,
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
    disp_pair,
    filter_text,
    explain_text,
    **properties,
    ):

    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    self.srv_strategy_title.content = serve_strategy_title
    self.player_label.text = self.player_drop_down.selected_value
    self.rpt_disp_box.content = table_data
    self.serve_plot.source = srv_plot
    self.pass_plot.source = pass_plot
    self.set_plot.source = set_plot
    self.z1_att_plot.source = z1_att
    self.z2_att_plot.source = z2_att
    self.z3_att_plot.source = z3_att
    self.z4_att_plot.source = z4_att
    self.z5_att_plot.source = z5_att
    self.opt_att_plot.source = opt_att
    self.z1_table.content = z1_tbl
    self.z2_table.content = z2_tbl
    self.z3_table.content = z3_tbl
    self.z4_table.content = z4_tbl
    self.z5_table.content = z5_tbl
    self.opt_table.content = opt_tbl
    self.pair_label.text = disp_pair
    self.rich_text_2.content = filter_text
    self.rpt_disp_box4.content = explain_text

    # Any code you write here will run before the form opens.
