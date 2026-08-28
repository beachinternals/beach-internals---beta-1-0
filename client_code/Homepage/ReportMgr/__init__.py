from ._anvil_designer import ReportMgrTemplate
from anvil import *
import anvil.server
import anvil.users
from anvil.tables import app_tables
import re

from ..DataMgr.btd_form_helpers import format_lgy, parse_lgy, get_league_items

# Front end for the rpt_mgr table (Homepage.ReportMgr). Every list/add/update/
# delete call is routed through server_code/rpt_mgr_admin.py, which scopes
# everything to the logged-in user's own team.
#
# The lgy (league/gender/year) dropdown is never stored on the rpt_mgr row --
# it only scopes which master_player rows are offered in the player_select
# picker. server_code/rpt_manager.py derives league/gender/year from whichever
# players end up linked in player_list.
#
# 'scouting' is intentionally left out of RPT_TYPE_CHOICES -- rpt_manager.py
# drives scouting reports from pair_list, which isn't supported by this UI yet.

DOW_CHOICES = ['Everyday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
RPT_TYPE_CHOICES = ['player', 'dashboard', 'league']
EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')


class ReportMgr(ReportMgrTemplate):
  def __init__(self, **properties):
    self.init_components(**properties)

    user = anvil.users.get_user(allow_remembered=True)
    if not user:
      alert('Please Sign In to Beach Internals')
      open_form('Homepage.UserMgr')
      return
    if not user['team']:
      alert('Please Contact Beach Internals to be Assigned to a Team')
      open_form('Homepage.Contact')
      return

    self.user = user
    self.mode = 'add'
    self.editing_id = None

    self.rpt_list_panel.set_event_handler('x-edit-report', self.rpt_list_panel_x_edit_report)

    self.init_form()

  def init_form(self):
    self.dow_dropdown.items = DOW_CHOICES
    self.rpt_type_dropdown.items = RPT_TYPE_CHOICES

    self.lgy_dropdown.items = get_league_items(self.user['team'])
    default_lgy = format_lgy(self.user['def_league'], self.user['def_gender'], self.user['def_year'])
    if default_lgy in self.lgy_dropdown.items:
      self.lgy_dropdown.selected_value = default_lgy
    elif self.lgy_dropdown.items:
      self.lgy_dropdown.selected_value = self.lgy_dropdown.items[0]

    self.refresh_player_choices()
    self.refresh_report_list()
    self.reset_to_add_mode()

  # ==========================================================================
  #  REPORT LIST
  # ==========================================================================

  def refresh_report_list(self):
    reports = anvil.server.call('list_rpt_mgr_reports')
    self.rpt_list_panel.items = reports
    self.report_count_label.text = f"{len(reports)} report(s)" if reports else "No reports yet"

  def rpt_list_panel_x_edit_report(self, item, **event_args):
    self.enter_edit_mode(item)

  # ==========================================================================
  #  PLAYER PICKER (scoped to the selected league/gender/year)
  # ==========================================================================

  def lgy_dropdown_change(self, **event_args):
    self.refresh_player_choices()

  def refresh_player_choices(self):
    league, gender, year = parse_lgy(self.lgy_dropdown.selected_value)
    players = anvil.server.call('get_rpt_player_choices', league, gender, year) if league else []
    self.player_select.items = [
      (f"{p['team']} {p['number']} {p['shortname']}", p['id']) for p in players
    ]

  # ==========================================================================
  #  INCLUDED-REPORTS PICKER (scoped to the selected report type)
  # ==========================================================================

  def rpt_type_dropdown_change(self, **event_args):
    self.refresh_rpts_inc_choices()

  def refresh_rpts_inc_choices(self):
    rpt_type = self.rpt_type_dropdown.selected_value
    reports = app_tables.report_list.search(private=False, rpt_type=rpt_type) if rpt_type else []
    self.rpts_inc_select.items = [(r['report_name'] or r['function_name'] or 'Unnamed report', r) for r in reports]

  # ==========================================================================
  #  MODE SWITCHING
  # ==========================================================================

  def reset_to_add_mode(self, **event_args):
    self.mode = 'add'
    self.editing_id = None

    self.mode_label.text = "Add New Report"
    self.description_box.text = ""
    self.dow_dropdown.selected_value = None
    self.rpt_type_dropdown.selected_value = None
    self.emailto_box.text = self.user['email']
    self.active_checkbox.checked = True
    self.player_select.selected = []
    self.rpts_inc_select.items = []
    self.rpts_inc_select.selected = []
    self.delete_button.visible = False
    self.primary_button.text = "Add Report"

  def enter_edit_mode(self, item):
    self.mode = 'edit'
    self.editing_id = item.get_id()

    self.mode_label.text = "Edit Report"
    self.description_box.text = item['report_description'] or ''
    self.dow_dropdown.selected_value = item['dow']
    self.emailto_box.text = item['emailto'] or self.user['email']
    self.active_checkbox.checked = item['active'] == 'Yes'

    rpt_type = item['rpt_type']
    if rpt_type in self.rpt_type_dropdown.items:
      self.rpt_type_dropdown.selected_value = rpt_type
    self.refresh_rpts_inc_choices()

    self.player_select.selected = [r.get_id() for r in (item['player_list'] or [])]
    self.rpts_inc_select.selected = list(item['rpts_inc'] or [])

    self.delete_button.visible = True
    self.primary_button.text = "Save Changes"

  def cancel_button_click(self, **event_args):
    self.reset_to_add_mode()

  # ==========================================================================
  #  SAVE / DELETE
  # ==========================================================================

  def primary_button_click(self, **event_args):
    if not (self.description_box.text or '').strip():
      alert("Please enter a report description.")
      return
    if not self.dow_dropdown.selected_value:
      alert("Please select a day of week.")
      return
    if not self.rpt_type_dropdown.selected_value:
      alert("Please select a report type.")
      return
    if not self.player_select.selected:
      alert("Select at least one player.")
      return
    if not EMAIL_RE.match((self.emailto_box.text or '').strip()):
      alert("Please enter a valid email address.")
      return

    if self.mode == 'add':
      self.do_add()
    else:
      self.do_edit()

  def do_add(self):
    result = anvil.server.call(
      'add_rpt_mgr_report',
      self.description_box.text,
      self.dow_dropdown.selected_value,
      self.rpt_type_dropdown.selected_value,
      self.emailto_box.text,
      self.player_select.selected,
      self.rpts_inc_select.selected,
      self.active_checkbox.checked,
    )
    if not result['success']:
      alert(result['message'])
      return

    Notification(result['message'], style='success', timeout=3).show()
    self.reset_to_add_mode()
    self.refresh_report_list()

  def do_edit(self):
    result = anvil.server.call(
      'update_rpt_mgr_report',
      self.editing_id,
      self.description_box.text,
      self.dow_dropdown.selected_value,
      self.rpt_type_dropdown.selected_value,
      self.emailto_box.text,
      self.player_select.selected,
      self.rpts_inc_select.selected,
      self.active_checkbox.checked,
    )
    if not result['success']:
      alert(result['message'])
      return

    Notification(result['message'], style='success', timeout=4).show()
    self.reset_to_add_mode()
    self.refresh_report_list()

  def delete_button_click(self, **event_args):
    if not self.editing_id:
      return
    description = self.description_box.text or 'this report'
    if not confirm(f'Delete "{description}"?\n\nThis cannot be undone.'):
      return

    result = anvil.server.call('delete_rpt_mgr_report', self.editing_id)
    if not result['success']:
      alert(result['message'])
      return

    Notification(result['message'], style='success', timeout=3).show()
    self.reset_to_add_mode()
    self.refresh_report_list()

  def close_button_click(self, **event_args):
    open_form('Homepage')
