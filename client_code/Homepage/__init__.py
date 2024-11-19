from ._anvil_designer import HomepageTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.users


class Homepage(HomepageTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # make admin visible, or not
    user_row = anvil.users.get_user()

    if user_row:
      self.user_name_label.text = user_row['team'] 
      if user_row['team'] == "INTERNALS":
        self.admin_link.visible = True
        self.pair_rpt_link.visible = True
        self.league_rpt_link.visible = True
        self.rpt_mgr_link.visible = True
      else:
        self.admin_link.visible = False
        self.pair_rpt_link.visible = False
        self.league_rpt_link.visible = False
        self.rpt_mgr_link.visible = False
        self.scouting_rpt_link.visible = False
    else:
      self.admin_link.visible = False
      self.pair_rpt_link.visible = False
      self.league_rpt_link.visible = False
      self.rpt_mgr_link.visible = False
      self.scouting_rpt_link.visible = False

    pass
    
  def datamgr_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.DataMgr')
    pass

  def playerRpt_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.PlayerRpt')
    pass

  def user_page_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.UserMgr')
    if anvil.users.get_user():
      self.user_name_label.text = anvil.users.get_user()['team']
    pass

  def help1_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.HelpPage')
    pass

  def aboutus_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.AboutUs')
    pass

  def subscription_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.SubscriptionMgr')
    pass

  def pair_rpt_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.PairRpt')
    pass

  def scouting_rpt_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.ScoutingRpt')
    pass

  def league_rpt_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.LeagueRpt')
    pass

  def rpt_mgr_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.ReportMgr')
    pass

  def subscription_mgr_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.SubscriptionMgr')
    pass

  def about_us_link2_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.AboutUs')
    pass

  def contact_us_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.Contact')
    pass

  def help_link2_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.HelpPage')
    pass

  def beta_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.BetaDevLog')
    pass

  def admin_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.admin')
    pass

  def home_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.Landing_form')
    pass

  def Contact_us_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.Contact')
    pass

  def signin_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.UserMgr')
    if anvil.users.get_user():
      self.user_name_label.text = anvil.users.get_user()['team']
    pass

  def dashboard_link_click(self, **event_args):
    """This method is called when the link is clicked"""
    open_form('Homepage.Dashboard')
    pass
