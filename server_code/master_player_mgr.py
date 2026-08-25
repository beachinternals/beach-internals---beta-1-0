import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.users
import anvil.server
import datetime
from server_functions import generate_player_uuid

# This is a server module for the Master Player File management page.
# It lets a user browse the roster for one league/gender/year, add a
# player, correct an existing player's team/number/short name/full name,
# merge two or three duplicate player records into one, or delete a
# player -- keeping btd_files.ppr_playerX name references in sync and
# queued for tonight's reprocessing whenever a player's identity changes.

_PPR_PLAYER_COLUMNS = ('ppr_playera1', 'ppr_playera2', 'ppr_playerb1', 'ppr_playerb2')


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


# ============================================================================
#  NORMALIZATION
# ============================================================================

def _normalize_team(team):
  return (team or '').strip().upper()

def _normalize_number(number):
  """number is always a string. A single-character number is zero-padded, so '5' becomes '05'."""
  number = (number or '').strip()
  if len(number) == 1:
    number = '0' + number
  return number

def _normalize_shortname(shortname):
  return (shortname or '').strip()

def _normalize_fullname(fullname):
  return (fullname or '').strip()

def _composed_name(team, number, shortname):
  """The 'TEAM NUMBER SHORTNAME' form stored in btd_files.ppr_playerX columns."""
  return f"{team} {number} {shortname}"


# ============================================================================
#  BTD FILES SYNC
# ============================================================================

def _rename_in_btd_files(league, gender, year, rename_map):
  """
  Repoint any ppr_playerX column matching a key in rename_map to its new
  composed name, scoped to a single league/gender/year. Bumps btd_file_date
  on every row touched so tonight's night_processing_cron reprocesses it.
  Returns the number of btd_files rows updated.
  """
  if not rename_map:
    return 0

  count = 0
  for row in app_tables.btd_files.search(league=league, gender=gender, year=year):
    updates = {col: rename_map[row[col]] for col in _PPR_PLAYER_COLUMNS if row[col] in rename_map}
    if updates:
      updates['btd_file_date'] = datetime.datetime.now()
      row.update(**updates)
      count += 1
  return count


def _count_btd_file_references(league, gender, year, composed_name):
  """How many btd_files rows (scoped to league/gender/year) reference composed_name in any of the 4 player columns."""
  count = 0
  for row in app_tables.btd_files.search(league=league, gender=gender, year=year):
    if any(row[col] == composed_name for col in _PPR_PLAYER_COLUMNS):
      count += 1
  return count


# ============================================================================
#  SEARCH
# ============================================================================

@anvil.server.callable
def search_master_players(league, gender, year, search_text):
  """
  Search the league/gender/year roster by a substring of team, number, or
  short name (matched against the same "TEAM NUMBER SHORTNAME" form stored
  in btd_files.ppr_playerX). Returns [] for a blank search rather than the
  whole roster.
  """
  _require_login()

  search_text = (search_text or '').strip().lower()
  if not search_text:
    return []

  players = []
  for row in app_tables.master_player.search(league=league, gender=gender, year=year):
    haystack = _composed_name(row['team'], row['number'], row['shortname']).lower()
    if search_text in haystack:
      players.append({
        'id': row.get_id(),
        'team': row['team'],
        'number': row['number'],
        'shortname': row['shortname'],
        'fullname': row['fullname'],
        'player_uuid': row['player_uuid'],
      })

  players.sort(key=lambda p: (p['team'] or '', p['number'] or '', p['shortname'] or ''))
  return players


# ============================================================================
#  ADD / UPDATE / MERGE / DELETE
# ============================================================================

@anvil.server.callable
def add_master_player(league, gender, year, team, number, shortname, fullname):
  """Add a new master_player row. number is a string; a 1-character number is zero-padded."""
  _require_login()

  team = _normalize_team(team)
  number = _normalize_number(number)
  shortname = _normalize_shortname(shortname)
  fullname = _normalize_fullname(fullname)

  if not team or not number or not shortname:
    return {'success': False, 'message': 'Team, number, and short name are all required.'}

  existing = app_tables.master_player.search(
    league=league, gender=gender, year=year, team=team, number=number, shortname=shortname
  )
  if len(existing) > 0:
    return {'success': False, 'message': f'Player "{_composed_name(team, number, shortname)}" already exists.'}

  add_row = app_tables.master_player.add_row(
    league=league, gender=gender, year=year,
    team=team, number=number, shortname=shortname, fullname=fullname,
  )
  add_row['player_uuid'] = generate_player_uuid()

  return {'success': True, 'message': f'Player "{_composed_name(team, number, shortname)}" added.'}


@anvil.server.callable
def update_master_player(player_id, team, number, shortname, fullname):
  """
  Save corrections to an existing player. If the team+number+shortname
  identity changed, every matching btd_files.ppr_playerX reference (scoped
  to this player's league/gender/year) is repointed to the new name and
  queued for tonight's reprocessing.
  """
  _require_login()

  row = app_tables.master_player.get_by_id(player_id)
  if not row:
    raise Exception("Player not found.")

  league, gender, year = row['league'], row['gender'], row['year']
  old_composed = _composed_name(row['team'], row['number'], row['shortname'])

  team = _normalize_team(team)
  number = _normalize_number(number)
  shortname = _normalize_shortname(shortname)
  fullname = _normalize_fullname(fullname)

  if not team or not number or not shortname:
    return {'success': False, 'message': 'Team, number, and short name are all required.'}

  new_composed = _composed_name(team, number, shortname)

  if new_composed != old_composed:
    collision = app_tables.master_player.search(
      league=league, gender=gender, year=year, team=team, number=number, shortname=shortname
    )
    if any(r.get_id() != player_id for r in collision):
      return {
        'success': False,
        'message': f'Player "{new_composed}" already exists. Use Merge instead if these are the same player.',
      }

  row.update(team=team, number=number, shortname=shortname, fullname=fullname)

  files_updated = 0
  if new_composed != old_composed:
    files_updated = _rename_in_btd_files(league, gender, year, {old_composed: new_composed})

  message = 'Player updated.'
  if files_updated:
    message += f' {files_updated} file(s) queued for reprocessing.'

  return {'success': True, 'files_updated': files_updated, 'message': message}


@anvil.server.callable
def merge_master_players(league, gender, year, player_ids, team, number, shortname, fullname):
  """
  Merge two or more master_player rows into a single corrected record.
  Every btd_files.ppr_playerX reference to any of the source players'
  composed names (scoped to this league/gender/year) is repointed to the
  new composed name and queued for tonight's reprocessing. The source rows
  (other than the one kept) are deleted, leaving exactly one row.
  """
  _require_login()

  player_ids = list(dict.fromkeys(player_ids or []))
  if len(player_ids) < 2:
    return {'success': False, 'message': 'Select at least two players to merge.'}

  rows = []
  for pid in player_ids:
    row = app_tables.master_player.get_by_id(pid)
    if not row:
      raise Exception("One or more selected players could not be found.")
    if row['league'] != league or row['gender'] != gender or row['year'] != year:
      raise Exception("Selected players are not all in the same league.")
    rows.append(row)

  team = _normalize_team(team)
  number = _normalize_number(number)
  shortname = _normalize_shortname(shortname)
  fullname = _normalize_fullname(fullname)

  if not team or not number or not shortname:
    return {'success': False, 'message': 'Team, number, and short name are all required.'}

  new_composed = _composed_name(team, number, shortname)
  old_composed_set = {_composed_name(r['team'], r['number'], r['shortname']) for r in rows}

  collision = app_tables.master_player.search(
    league=league, gender=gender, year=year, team=team, number=number, shortname=shortname
  )
  if any(r.get_id() not in player_ids for r in collision):
    return {'success': False, 'message': f'Player "{new_composed}" already exists outside the selected group.'}

  target = rows[0]
  target.update(team=team, number=number, shortname=shortname, fullname=fullname)
  for row in rows[1:]:
    row.delete()

  rename_map = {old: new_composed for old in old_composed_set if old != new_composed}
  files_updated = _rename_in_btd_files(league, gender, year, rename_map)

  message = f'Merged {len(rows)} players into "{new_composed}".'
  if files_updated:
    message += f' {files_updated} file(s) queued for reprocessing.'

  return {'success': True, 'files_updated': files_updated, 'message': message}


@anvil.server.callable
def delete_master_player(player_id):
  """Delete a player, but only if no btd_files row (in their league/gender/year) still references them."""
  _require_login()

  row = app_tables.master_player.get_by_id(player_id)
  if not row:
    raise Exception("Player not found.")

  league, gender, year = row['league'], row['gender'], row['year']
  composed = _composed_name(row['team'], row['number'], row['shortname'])

  references = _count_btd_file_references(league, gender, year, composed)
  if references:
    return {
      'success': False,
      'message': f'Cannot delete "{composed}": referenced by {references} Ball Time file(s). '
                 f'Reassign or merge those files first.',
    }

  row.delete()
  return {'success': True, 'message': f'Player "{composed}" deleted.'}
