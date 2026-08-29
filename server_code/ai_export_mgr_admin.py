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

DOW_CHOICES = ['Everyday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']


def _require_login():
  """Verify the caller is logged in and assigned to a team. Returns the user row."""
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if not user['team']:
    raise Exception("Your account is not assigned to a team. Please contact Beach Internals.")
  return user


def _export_field_error(note, dow, datasets_included):
  """None if note/dow/datasets_included are all valid, else an error result dict."""
  if not note:
    return {'success': False, 'message': 'Please enter a title for this export.'}
  if dow not in DOW_CHOICES:
    return {'success': False, 'message': 'Please select a day of week.'}
  if not datasets_included:
    return {'success': False, 'message': 'Select at least one dataset to include.'}
  return None


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
  Players for the player_filter multi-select: the union of master_player rows
  across every (gender, year) the logged-in user's team is subscribed to
  under this league. Returns plain dicts, not linked rows, to keep the
  payload light -- add_ai_export / update_ai_export take player ids and
  resolve them back to rows server-side. The client's MultiSelectDropDown
  provides its own filter box, so this loads the whole league roster once
  per league selection rather than per keystroke.
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
      if row_id in seen_ids:
        continue
      seen_ids.add(row_id)
      players.append({
        'id': row_id,
        'team': row['team'],
        'number': row['number'],
        'shortname': row['shortname'],
      })

  players.sort(key=lambda p: (p['team'] or '', p['number'] or '', p['shortname'] or ''))
  return players


@anvil.server.callable
def get_export_datasets():
  """
  All rows from ai_export_dataset_list, for the datasets_included multi-select,
  sorted the same way ai_export_dataset_combiner.get_enabled_datasets() orders
  a saved export's datasets. Not filtered by 'active' -- that column isn't
  used by the export pipeline itself, so filtering on it here would just hide
  real dataset options.
  """
  _require_login()
  datasets = list(app_tables.ai_export_dataset_list.search())
  datasets.sort(key=lambda r: r['order'] or 0)
  return datasets


# ============================================================================
#  LIST / ADD / UPDATE / DELETE
# ============================================================================

@anvil.server.callable
def list_ai_exports():
  """This team's ai_export_mgr rows -- every team's rows if the caller is INTERNALS."""
  user = _require_login()
  if user['team'] == 'INTERNALS':
    rows = list(app_tables.ai_export_mgr.search())
  else:
    rows = list(app_tables.ai_export_mgr.search(team=user['team']))
  rows.sort(key=lambda r: (r['Note'] or '').lower())
  return rows


@anvil.server.callable
def add_ai_export(league, note, dow, player_filter_ids, datasets_included, disabled):
  user = _require_login()

  note = (note or '').strip()
  error = _export_field_error(note, dow, datasets_included)
  if error:
    return error
  _validate_league_params(league=league, team=user['team'])

  player_rows = [r for r in (app_tables.master_player.get_by_id(pid) for pid in (player_filter_ids or [])) if r]

  app_tables.ai_export_mgr.add_row(
    league=league,
    team=user['team'],
    Note=note,
    export_type='markdown',
    dow=dow,
    player_filter=player_rows,  # type: ignore[reportArgumentType]  -- link_multiple accepts any list of rows, stub is overly narrow
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
def update_ai_export(export_id, league, note, dow, player_filter_ids, datasets_included, disabled):
  _require_login()

  row = app_tables.ai_export_mgr.get_by_id(export_id)
  if not row:
    raise Exception("Export not found.")
  _require_own_team(row['team'])

  note = (note or '').strip()
  error = _export_field_error(note, dow, datasets_included)
  if error:
    return error
  _validate_league_params(league=league, team=row['team'])

  player_rows = [r for r in (app_tables.master_player.get_by_id(pid) for pid in (player_filter_ids or [])) if r]

  row.update(
    league=league,
    Note=note,
    dow=dow,
    player_filter=player_rows,  # type: ignore[reportArgumentType]  -- link_multiple accepts any list of rows, stub is overly narrow
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
