from ._anvil_designer import team_quality_emailTemplate
from anvil import *
import anvil.server
import anvil.users
import re

from ..btd_form_helpers import format_lgy, parse_lgy, get_league_items

# Front end for a self-service "email me my team's data quality report" page
# (Homepage.DataMgr.team_quality_email), linked from the DataMgr nav right
# under "Manage Uploaded Matches". Always scoped to the logged-in user's own
# team -- server_code/weekly_data_quality_report.py's
# trigger_team_corrections_report lets INTERNALS request any team, but
# everyone else only their own (enforced server-side).

_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')


class team_quality_email(team_quality_emailTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    user = anvil.users.get_user(allow_remembered=True)
    if not user:
      alert("Not Currently Logged in")
      open_form('Homepage.Landing_form')
      return
    if not user['team']:
      alert('Please Contact Beach Internals to be Assigned to a Team')
      open_form('Homepage.Contact')
      return

    self.user = user
    self.init_form()

  def init_form(self):
    self.lgy_dropdown.items = get_league_items(self.user['team'])
    default_lgy = format_lgy(self.user['def_league'], self.user['def_gender'], self.user['def_year'])
    if default_lgy in self.lgy_dropdown.items:
      self.lgy_dropdown.selected_value = default_lgy
    elif self.lgy_dropdown.items:
      self.lgy_dropdown.selected_value = self.lgy_dropdown.items[0]

    self.email_box.text = self.user['email']

  def send_button_click(self, **event_args):
    league, gender, year = parse_lgy(self.lgy_dropdown.selected_value)
    if not league:
      alert("Select a league / gender / year.")
      return

    email = (self.email_box.text or '').strip()
    if not _EMAIL_RE.match(email):
      alert("Please enter a valid email address.")
      return

    task = anvil.server.call(
      'trigger_team_corrections_report',
      league,
      gender,
      year,
      self.user['team'],
      self.start_date_picker.date,
      self.end_date_picker.date,
      email,
    )
    alert(f"{task['status']}. Check {email} shortly.")

  def close_button_click(self, **event_args):
    open_form('Homepage.DataMgr')
