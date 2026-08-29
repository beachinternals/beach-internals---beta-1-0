import anvil.server
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.users
import re

# This is a server module for the Report Manager front end (Homepage.ReportMgr).
# It lets a user browse, add, edit, and delete their own team's rpt_mgr rows.
#
# pair_list, pair_b_list, and the filter columns (comp_l1..copy_beachinternals)
# are not exposed in the UI and are never written here -- pair_list/pair_b_list
# are deprecated (no data), and the filter columns are deferred to a future pass.
#
# 'scouting' is intentionally left out of RPT_TYPE_CHOICES: server_code/rpt_manager.py
# drives scouting reports from pair_list, which isn't supported by this UI yet.
#
# league/gender/year (the "lgy" dropdown on the client) is never stored on the
# row -- rpt_manager.py derives that information from whichever master_player
# rows are linked in player_list. The lgy dropdown here only scopes which
# players are available to pick.
#
# 'active' is stored as the string "Yes"/"No", matching
# server_code/rpt_manager.py's rpt_mgr_generate_background, which does
# app_tables.rpt_mgr.search(active="Yes").

DOW_CHOICES = ['Everyday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
RPT_TYPE_CHOICES = ['player', 'dashboard', 'league']

_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')


def _require_login():
  """Verify the caller is logged in and assigned to a team. Returns the user row."""
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if not user['team']:
    raise Exception("Your account is not assigned to a team. Please contact Beach Internals.")
  return user


def _require_own_team(team):
  """Verify the caller is logged in AND is either INTERNALS or requesting their own team's data."""
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if user['team'] != 'INTERNALS' and team != user['team']:
    raise Exception("Access denied: you can only access your own team's data.")
  return user


def _email_looks_valid(email):
  return bool(email) and bool(_EMAIL_RE.match(email))


def _rpt_field_error(description, dow, rpt_type, emailto, player_ids):
  """None if all fields are valid, else an error result dict."""
  if not description:
    return {'success': False, 'message': 'Please enter a report description.'}
  if dow not in DOW_CHOICES:
    return {'success': False, 'message': 'Please select a day of week.'}
  if rpt_type not in RPT_TYPE_CHOICES:
    return {'success': False, 'message': 'Please select a report type.'}
  if not player_ids:
    return {'success': False, 'message': 'Select at least one player.'}
  if not _email_looks_valid(emailto):
    return {'success': False, 'message': 'Please enter a valid email address.'}
  return None


# ============================================================================
#  DROPDOWN / PICKER DATA
# ============================================================================

@anvil.server.callable
def get_rpt_player_choices(league, gender, year):
  """
  All master_player rows for this exact league/gender/year, for the
  player_list multi-select. Returns plain dicts, not linked rows --
  add_rpt_mgr_report / update_rpt_mgr_report take player ids and resolve
  them back to rows server-side.
  """
  _require_login()

  players = [
    {'id': row.get_id(), 'team': row['team'], 'number': row['number'], 'shortname': row['shortname']}
    for row in app_tables.master_player.search(league=league, gender=gender, year=year)
  ]
  players.sort(key=lambda p: (p['team'] or '', p['number'] or '', p['shortname'] or ''))
  return players


# ============================================================================
#  LIST / ADD / UPDATE / DELETE
# ============================================================================

@anvil.server.callable
def list_rpt_mgr_reports():
  """This team's rpt_mgr rows -- every team's rows if the caller is INTERNALS."""
  user = _require_login()
  if user['team'] == 'INTERNALS':
    rows = list(app_tables.rpt_mgr.search())
  else:
    rows = list(app_tables.rpt_mgr.search(team=user['team']))
  rows.sort(key=lambda r: (r['report_description'] or '').lower())
  return rows


@anvil.server.callable
def add_rpt_mgr_report(description, dow, rpt_type, emailto, player_ids, rpts_inc, active):
  user = _require_login()

  description = (description or '').strip()
  emailto = (emailto or '').strip()
  error = _rpt_field_error(description, dow, rpt_type, emailto, player_ids)
  if error:
    return error

  player_rows = [r for r in (app_tables.master_player.get_by_id(pid) for pid in (player_ids or [])) if r]

  app_tables.rpt_mgr.add_row(
    team=user['team'],
    email=user['email'],
    emailto=emailto,
    report_description=description,
    dow=dow,
    rpt_type=rpt_type,
    player_list=player_rows,
    rpts_inc=rpts_inc or [],
    active='Yes' if active else 'No',
  )
  return {'success': True, 'message': f'Report "{description}" added.'}


@anvil.server.callable
def update_rpt_mgr_report(report_id, description, dow, rpt_type, emailto, player_ids, rpts_inc, active):
  _require_login()

  row = app_tables.rpt_mgr.get_by_id(report_id)
  if not row:
    raise Exception("Report not found.")
  _require_own_team(row['team'])

  description = (description or '').strip()
  emailto = (emailto or '').strip()
  error = _rpt_field_error(description, dow, rpt_type, emailto, player_ids)
  if error:
    return error

  player_rows = [r for r in (app_tables.master_player.get_by_id(pid) for pid in (player_ids or [])) if r]

  row.update(
    emailto=emailto,
    report_description=description,
    dow=dow,
    rpt_type=rpt_type,
    player_list=player_rows,
    rpts_inc=rpts_inc or [],
    active='Yes' if active else 'No',
  )
  return {'success': True, 'message': f'Report "{description}" updated.'}


@anvil.server.callable
def delete_rpt_mgr_report(report_id):
  _require_login()

  row = app_tables.rpt_mgr.get_by_id(report_id)
  if not row:
    raise Exception("Report not found.")
  _require_own_team(row['team'])

  description = row['report_description']
  row.delete()
  return {'success': True, 'message': f'Report "{description}" deleted.'}
