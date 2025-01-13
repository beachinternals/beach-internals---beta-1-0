from ._anvil_designer import LeagueRptTemplate
from anvil import *
import plotly.graph_objects as go
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from LeagueRpt1 import *
from LeagueRpt2 import *



class LeagueRpt(LeagueRptTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.
    # Check for login
    user_row = anvil.users.get_user(allow_remembered=True)
    if not user_row:
      alert('Please Sign In to Beach Internals')
      open_form('Homepage.UserMgr')
    elif not user_row["team"]:
      alert('Please Contact Beach Internals to be Assigned to a Team')
      open_form('Homepage.Contact')

    # set up the league drop down
    self.league_drop_down.selected_value = user_row["def_league"]+'|'+user_row['def_gender']+'|'+user_row['def_year']
    self.league_drop_down.items = list(set([(r['league'])+' | '+r['gender']+' | '+r['year'] for r in app_tables.subscriptions.search(team=user_row['team'])]))

    pass


  def generate_report_button_click(self, **event_args):
    """This method is called when the button is clicked"""
    stat_text, hist_plot = anvil.server.call('plot_histogram',self.league_drop_down.selected_value,'fbhe','First Ball Hitting Efficiency')
    self.data_table_1.content = stat_text
    self.fbhe_histogram_plot.source = hist_plot

    ed_text, ed_plot = anvil.server.call('plot_histogram',self.league_drop_down.selected_value,'err_den','Error Density')
    self.ed_stats.content = ed_text
    self.ed_plot.source = ed_plot

    tcr_text, tcr_plot = anvil.server.call('plot_histogram',self.league_drop_down.selected_value,'tcr', 'Transition Conversion Rate')
    self.tcr_stats.content = tcr_text
    self.tcr_plot.source = tcr_plot

    ev_text, ev_plot = anvil.server.call('plot_histogram',self.league_drop_down.selected_value,'expected','Expected Value')
    self.ev_stats.content = ev_text
    self.ev_plot.source = ev_plot
    
    pass
