from anvil.tables import app_tables

# Shared dropdown-population and BTD-upload-result helpers used by both
# btd_import (new file upload) and DataMgr.btd_manage (correcting an
# existing file), so the league/competition/venue/roster lookups and the
# BTD statistics unpacking only live in one place.


def parse_lgy(lgy_string):
  """Parse 'NCAA | W | 2025' into (league, gender, year). Returns (None, None, None) on bad input."""
  if not lgy_string or '|' not in lgy_string:
    return None, None, None
  parts = [p.strip() for p in lgy_string.split('|')]
  if len(parts) != 3:
    return None, None, None
  return parts[0], parts[1], parts[2]


def format_lgy(league, gender, year):
  """Build the 'NCAA | W | 2025' dropdown display/value string."""
  return f"{league} | {gender} | {year}"


def get_league_items(team):
  """Sorted 'LEAGUE | GENDER | YEAR' options for a team's subscriptions, newest year first."""
  items = {
    format_lgy(row['league'], row['gender'], row['year'])
    for row in app_tables.subscriptions.search(team=team)
  }
  return sorted(items, reverse=True)


def get_comp_l1_items(league):
  return [(row['comp_l1'], row) for row in app_tables.league_comp_l1.search(league=league)]


def get_comp_l2_items(league, comp_l1):
  return [(row['comp_l2'], row) for row in app_tables.league_comp_l2.search(league=league, comp_l1=comp_l1)]


def get_comp_l3_items(league):
  """comp_l3 options are keyed off comp_l3_label, which league_list maps to for this league."""
  comp3lbl = [row['comp_l3_label'] for row in app_tables.league_list.search(league=league)]
  if not comp3lbl:
    return []
  return [(row['comp_l3'], row) for row in app_tables.league_comp_l3.search(comp_l3_label=comp3lbl[0])]


def get_venue_items(league):
  return [(row['venue_name'], row) for row in app_tables.venue.search(venue_league=league)]


def get_ppr_player_list(league, gender, year):
  """Sorted 'TEAM NUMBER SHORTNAME' roster strings for the ppr_playerX mapping dropdowns."""
  players = [
    f"{row['team']} {row['number']} {row['shortname']}"
    for row in app_tables.master_player.search(league=league, year=year, gender=gender)
  ]
  players.sort()
  return players


def unpack_btd_statistics(statistics, cleaned_csv, filename):
  """
  Unpack the (statistics, cleaned_csv, error_message) result of
  anvil.server.call('update_btd_characteristics', file) -- once the caller
  has confirmed error_message is None -- into a plain dict ready to
  populate the BTD-player dropdowns, stat labels, and a save payload.
  """
  playera1, playera2, playerb1, playerb2, num_serves, comp_score, per_action_players, per_coord, per_srv_players = statistics
  return {
    'players': [playera1, playera2, playerb1, playerb2],
    'cleaned_csv': cleaned_csv,
    'filename': filename,
    'points': num_serves,
    'completeness_score': float(comp_score.rstrip('%')),
    'per_players': float(per_action_players.rstrip('%')),
    'per_xy': float(per_coord.rstrip('%')),
    'per_srv_players': float(per_srv_players.rstrip('%')),
  }
