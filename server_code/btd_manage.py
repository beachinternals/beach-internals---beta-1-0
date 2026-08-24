import anvil.files
from anvil.files import data_files
import anvil.secrets
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.users
import anvil.server
import datetime
from btd_upload import update_btd_characteristics

# This is a server module for BTD file management.
# It lets a team locate an already-uploaded BTD actions file, replace its
# CSV with a corrected export from Balltime, fix its metadata / player
# mapping, or delete it outright.

# ============================================================================
#  AUTH HELPER
# ============================================================================

def _require_login():
  """Verify the caller is logged in and assigned to a team. Returns the user row."""
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if not user['team']:
    raise Exception("Your account is not assigned to a team. Please contact Beach Internals.")
  return user

def _require_file_access(btd_file_id, user):
  """Load a btd_files row and confirm the logged-in user's team may manage it."""
  row = app_tables.btd_files.get_by_id(btd_file_id)
  if not row:
    raise Exception("BTD file not found.")
  if user['team'] != 'INTERNALS' and row['team'] != user['team']:
    raise Exception("You do not have access to this file.")
  return row


# ============================================================================
#  SEARCH / LIST
# ============================================================================

@anvil.server.callable
def get_btd_files_filtered(filters=None):
  """
  Get a filtered, sorted list of BTD files for the management screen, scoped
  to the caller's team (INTERNALS may see every team).

  filters = {
      'league': 'NCAA', 'gender': 'W', 'year': '2025',
      'date_from': date or None, 'date_to': date or None,
      'status': 'Private' | 'Scouting' | 'All',
      'min_completeness': 0-100 or None,
      'has_errors': True/False/None,
      'filename_search': substring or None,
      'sort_by': 'date' | 'upload_date' | 'completeness' | 'per_xy' | 'errors',
      'sort_desc': True/False,
  }
  """
  user = _require_login()
  filters = filters or {}

  query_args = {}
  if user['team'] != 'INTERNALS':
    query_args['team'] = user['team']
  if filters.get('league'):
    query_args['league'] = filters['league']
  if filters.get('gender'):
    query_args['gender'] = filters['gender']
  if filters.get('year'):
    query_args['year'] = filters['year']

  rows = app_tables.btd_files.search(**query_args)

  file_list = []
  for row in rows:
    if filters.get('date_from') and (not row['date'] or row['date'] < filters['date_from']):
      continue
    if filters.get('date_to') and (not row['date'] or row['date'] > filters['date_to']):
      continue

    if filters.get('status') and filters['status'] != 'All':
      if filters['status'] == 'Private' and not row['private']:
        continue
      if filters['status'] == 'Scouting' and row['private']:
        continue

    if filters.get('min_completeness'):
      if (row['completeness_score'] or 0) < filters['min_completeness']:
        continue

    if filters.get('has_errors'):
      if not row['no_errors']:
        continue

    if filters.get('filename_search'):
      needle = filters['filename_search'].strip().lower()
      if needle and needle not in (row['filename'] or '').lower():
        continue

    file_list.append({
      'id': row.get_id(),
      'date': row['date'],
      'league': row['league'],
      'gender': row['gender'],
      'year': row['year'],
      'comp_l1': row['comp_l1'],
      'comp_l2': row['comp_l2'],
      'comp_l3': row['comp_l3'],
      'team': row['team'],
      'filename': row['filename'],
      'points': row['points'],
      'completeness_score': row['completeness_score'],
      'per_xy': row['per_xy'],
      'per_srv_players': row['per_srv_players'],
      'per_players': row['per_players'],
      'private': row['private'],
      'no_errors': row['no_errors'] or 0,
      'error_str': row['error_str'] or '',
      'player1': row['player1'],
      'player2': row['player2'],
      'player3': row['player3'],
      'player4': row['player4'],
      'ppr_playera1': row['ppr_playera1'],
      'ppr_playera2': row['ppr_playera2'],
      'ppr_playerb1': row['ppr_playerb1'],
      'ppr_playerb2': row['ppr_playerb2'],
      'btd_file_date': row['btd_file_date'],
    })

  sort_by = filters.get('sort_by', 'date')
  sort_desc = filters.get('sort_desc', True)
  min_date = datetime.date(1900, 1, 1)
  min_datetime = datetime.datetime(1900, 1, 1)

  if sort_by == 'upload_date':
    file_list.sort(key=lambda x: x['btd_file_date'] or min_datetime, reverse=sort_desc)
  elif sort_by == 'completeness':
    file_list.sort(key=lambda x: x['completeness_score'] or 0, reverse=sort_desc)
  elif sort_by == 'per_xy':
    file_list.sort(key=lambda x: x['per_xy'] or 0, reverse=sort_desc)
  elif sort_by == 'errors':
    file_list.sort(key=lambda x: x['no_errors'], reverse=sort_desc)
  else:
    file_list.sort(key=lambda x: x['date'] or min_date, reverse=sort_desc)

  return file_list


@anvil.server.callable
def get_btd_file_details(btd_file_id):
  """Load complete details of a selected BTD file for the edit panel."""
  user = _require_login()
  row = _require_file_access(btd_file_id, user)

  return {
    'id': row.get_id(),
    'date': row['date'],
    'league': row['league'],
    'gender': row['gender'],
    'year': row['year'],
    'comp_l1': row['comp_l1'],
    'comp_l2': row['comp_l2'],
    'comp_l3': row['comp_l3'],
    'team': row['team'],
    'filename': row['filename'],
    'points': row['points'],
    'completeness_score': row['completeness_score'],
    'per_xy': row['per_xy'],
    'per_srv_players': row['per_srv_players'],
    'per_players': row['per_players'],
    'private': row['private'],
    'no_errors': row['no_errors'] or 0,
    'error_str': row['error_str'] or '',
    'player1': row['player1'],
    'player2': row['player2'],
    'player3': row['player3'],
    'player4': row['player4'],
    'ppr_playera1': row['ppr_playera1'],
    'ppr_playera2': row['ppr_playera2'],
    'ppr_playerb1': row['ppr_playerb1'],
    'ppr_playerb2': row['ppr_playerb2'],
    'venue_name': row['venue_name'],
    'venue_id': row['venue_id'],
    'match_time': row['match_time'],
    'btd_file_date': row['btd_file_date'],
    'ppr_file_date': row['ppr_file_date'],
  }


# ============================================================================
#  REPLACE / SAVE / DELETE
# ============================================================================

@anvil.server.callable
def check_replacement_btd_file(btd_file_id, new_csv_file):
  """
  Quick check on a candidate replacement actions file, using the same
  routine btd_import runs on a brand-new upload. Does not save anything.

  Returns: {'valid': True, 'statistics': [...], 'cleaned_csv': Media} or
           {'valid': False, 'message': str}
  """
  user = _require_login()
  _require_file_access(btd_file_id, user)

  statistics, cleaned_csv, error_message = update_btd_characteristics(new_csv_file)
  if error_message:
    return {'valid': False, 'message': error_message}
  return {'valid': True, 'statistics': statistics, 'cleaned_csv': cleaned_csv}


@anvil.server.callable
def update_existing_btd_file(btd_file_id, fields):
  """
  Save corrections to an existing BTD file: metadata, player mapping, and
  optionally a replacement actions CSV already validated by
  check_replacement_btd_file(). Always bumps btd_file_date so tonight's
  night_processing_cron picks the file up for reprocessing.

  fields = {
      'comp_l1', 'comp_l2', 'comp_l3', 'date', 'private',
      'venue_name', 'venue_id', 'match_time',
      'player1', 'player2', 'player3', 'player4',
      'ppr_playera1', 'ppr_playera2', 'ppr_playerb1', 'ppr_playerb2',
      # only present when the actions file was replaced:
      'csv_data', 'filename', 'points', 'completeness_score',
      'per_players', 'per_xy', 'per_srv_players',
  }
  """
  user = _require_login()
  row = _require_file_access(btd_file_id, user)

  editable_fields = [
    'comp_l1', 'comp_l2', 'comp_l3', 'date', 'private',
    'venue_name', 'venue_id', 'match_time',
    'player1', 'player2', 'player3', 'player4',
    'ppr_playera1', 'ppr_playera2', 'ppr_playerb1', 'ppr_playerb2',
    'csv_data', 'filename', 'points', 'completeness_score',
    'per_players', 'per_xy', 'per_srv_players',
  ]
  updates = {k: fields[k] for k in editable_fields if k in fields}
  updates['btd_file_date'] = datetime.datetime.now()
  row.update(**updates)

  return {'success': True, 'message': 'File updated. It will be reprocessed in tonight\'s run.'}


@anvil.server.callable
def delete_btd_file(btd_file_id):
  """Delete a BTD file from the system."""
  user = _require_login()
  row = _require_file_access(btd_file_id, user)

  filename = row['filename']
  row.delete()

  return {'success': True, 'message': f'BTD file "{filename}" deleted successfully'}
