from ._anvil_designer import HomepageTemplate
from anvil import *
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class Homepage(HomepageTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    # Any code you write here will run before the form opens.

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
    open_form('Homepage.SignIn')
    pass
