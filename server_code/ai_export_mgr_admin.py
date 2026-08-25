import anvil.server
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.users
from datetime import datetime

from ai_export_manager import _require_own_team, _validate_league_params

# This is a server module for the AI Export Manager front end
# (Homepage.DataMgr.ai_export_mgr). It lets a user browse, add, edit, and
# delete their own team's ai_export_mgr rows.
#
# Several ai_export_mgr columns are not exposed in the UI and are always set
# here to a fixed value: export_type='markdown', de_identified=True,
# ai_optimized=True, output_location='data_table'. Progress columns
# (status, created_at, started_at, completed_at, files_generated, file_list,
# result_message) are owned by ai_export_manager's background job and are
# never written here except created_at/files_generated on initial add.

DOW_CHOICES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']


def _require_login():
  """Verify the caller is logged in and assigned to a team. Returns the user row."""
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if not user['team']:
    raise Exception("Your account is not assigned to a team. Please contact Beach Internals.")
  return user


def _validate_export_fields(note, dow, datasets_included):
  note = (note or '').strip()
  if not note:
    return None, {'success': False, 'message': 'Please enter a title for this export.'}
  if dow not in DOW_CHOICES:
    return None, {'success': False, 'message': 'Please select a day of week.'}
  if not datasets_included:
    return None, {'success': False, 'message': 'Select at least one dataset to include.'}
  return note, None


# ============================================================================
#  DROPDOWN / PICKER DATA
# ============================================================================

@anvil.server.callable
def get_export_league_choices():
  """Distinct leagues the logged-in user's team is subscribed to."""
  user = _require_login()
  leagues = {row['league'] for row in app_tables.subscriptions.search(team=user['team'])}
  return sorted(leagues)


@anvil.server.callable
def get_export_players(league):
  """
  Players selectable for player_filter: the union of master_player rows
  across every (gender, year) the logged-in user's team is subscribed to
  under this league.
  """
  user = _require_login()
  _validate_league_params(league=league, team=user['team'])

  lgy_set = {
    (row['gender'], row['year'])
    for row in app_tables.subscriptions.search(team=user['team'], league=league)
  }

  seen_ids = set()
  players = []
  for gender, year in lgy_set:
    for row in app_tables.master_player.search(league=league, gender=gender, year=year):
      row_id = row.get_id()
      if row_id not in seen_ids:
        seen_ids.add(row_id)
        players.append(row)

  players.sort(key=lambda r: (r['team'] or '', r['number'] or '', r['shortname'] or ''))
  return players


@anvil.server.callable
def get_export_datasets():
  """Active rows from ai_export_dataset_list, for the datasets_included multi-select."""
  _require_login()
  datasets = list(app_tables.ai_export_dataset_list.search(active=True))
  datasets.sort(key=lambda r: r['dataset_name'] or '')
  return datasets


# ============================================================================
#  LIST / ADD / UPDATE / DELETE
# ============================================================================

@anvil.server.callable
def list_ai_exports():
  """This team's ai_export_mgr rows."""
  user = _require_login()
  rows = list(app_tables.ai_export_mgr.search(team=user['team']))
  rows.sort(key=lambda r: (r['Note'] or '').lower())
  return rows


@anvil.server.callable
def add_ai_export(league, note, dow, player_filter, datasets_included, disabled):
  user = _require_login()

  note, error = _validate_export_fields(note, dow, datasets_included)
  if error:
    return error
  _validate_league_params(league=league, team=user['team'])

  app_tables.ai_export_mgr.add_row(
    league=league,
    team=user['team'],
    Note=note,
    export_type='markdown',
    dow=dow,
    player_filter=player_filter,
    datasets_included=datasets_included,
    user_email=user['email'],
    de_identified=True,
    disabled=bool(disabled),
    ai_optimized=True,
    output_location='data_table',
    created_at=datetime.now(),
    files_generated=0,
  )
  return {'success': True, 'message': f'Export "{note}" added.'}


@anvil.server.callable
def update_ai_export(export_id, league, note, dow, player_filter, datasets_included, disabled):
  _require_login()

  row = app_tables.ai_export_mgr.get_by_id(export_id)
  if not row:
    raise Exception("Export not found.")
  _require_own_team(row['team'])

  note, error = _validate_export_fields(note, dow, datasets_included)
  if error:
    return error
  _validate_league_params(league=league, team=row['team'])

  row.update(
    league=league,
    Note=note,
    dow=dow,
    player_filter=player_filter,
    datasets_included=datasets_included,
    disabled=bool(disabled),
  )
  return {'success': True, 'message': f'Export "{note}" updated.'}


@anvil.server.callable
def delete_ai_export(export_id):
  _require_login()

  row = app_tables.ai_export_mgr.get_by_id(export_id)
  if not row:
    raise Exception("Export not found.")
  _require_own_team(row['team'])

  note = row['Note']
  row.delete()
  return {'success': True, 'message': f'Export "{note}" deleted.'}
