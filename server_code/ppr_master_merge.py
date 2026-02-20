import anvil.secrets
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.users
import anvil.server
import pandas as pd
import io
import math
from datetime import datetime, timedelta, date

from logger_utils import log_info, log_error

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.
#

# =============================================================================
#
#   deidentify_league_ppr
#
#   Replace player name identifiers with player_uuid values in the League ppr
#   dataframe, BEFORE writing it to ppr_csv_tables.
#
#   Only called when data_set == 'League'.
#
#   Columns substituted directly (one player name per cell):
#     player_a1, player_a2, player_b1, player_b2
#     serve_player, pass_player, att_player, dig_player
#
#   Columns substituted by string replace (concatenated pair strings):
#     teama, teamb, point_outcome_team
#
#   If a player appears in the ppr but is NOT in master_player:
#     - This is a data error (should not happen)
#     - Log the error and leave the real name in place (visible as an error marker)
#     - Do NOT raise an exception — keep processing the rest
#
# =============================================================================

def deidentify_league_ppr(master_ppr_df, c_league, c_gender, c_year):
  """
  De-identify the League ppr dataframe by replacing player names with UUIDs.

  Args:
    master_ppr_df: The assembled League ppr DataFrame (all teams, all matches)
    c_league:      League string e.g. 'NCAAW'
    c_gender:      Gender string e.g. 'W'
    c_year:        Year - may be string or int, will be cast to int

  Returns:
    DataFrame with player identifiers replaced by player_uuid values
  """

  log_info(f"deidentify_league_ppr: starting for {c_league}/{c_gender}/{c_year}, rows={master_ppr_df.shape[0]}")

  if master_ppr_df.empty:
    log_info("deidentify_league_ppr: empty dataframe passed, nothing to do")
    return master_ppr_df

  # -------------------------------------------------------------------------
  # Build name -> UUID substitution map from master_player
  # Key:   "FSU 12 Sara"   (team + ' ' + number + ' ' + shortname)
  # Value: "PLYR-a3f7c2e9"
  # -------------------------------------------------------------------------
  to_uuid = {}
  missing_players = set()   # track unmapped names so we log each only once

  try:
    player_rows = app_tables.master_player.search(
      league=c_league,
      gender=c_gender,
      year=int(c_year)
    )

    for row in player_rows:
      if not row['player_uuid']:
        log_error(
          f"deidentify_league_ppr: player has no UUID — run backfill_player_uuids(): "
          f"{row['team']} {row['number']} {row['shortname']}"
        )
        continue
      real_id = f"{row['team']} {row['number']} {row['shortname']}".strip()
      to_uuid[real_id] = row['player_uuid'].strip()

    log_info(f"deidentify_league_ppr: substitution map built — {len(to_uuid)} players")

  except Exception as e:
    log_error(f"deidentify_league_ppr: failed to build substitution map: {e}. Returning unmodified dataframe.")
    return master_ppr_df

  if not to_uuid:
    log_error(
      f"deidentify_league_ppr: no players found in master_player for "
      f"{c_league}/{c_gender}/{c_year}. Returning unmodified dataframe."
    )
    return master_ppr_df

  # -------------------------------------------------------------------------
  # Helper: substitute a single player-name cell
  # -------------------------------------------------------------------------
  def sub_player(val):
    if not isinstance(val, str) or not val.strip():
      return val
    player_id = val.strip()
    if player_id in to_uuid:
      return to_uuid[player_id]
    # Not found in master_player — data error
    if player_id and player_id not in missing_players:
      log_error(
        f"deidentify_league_ppr: DATA ERROR — player in ppr not in master_player: '{player_id}' "
        f"({c_league}/{c_gender}/{c_year}). Left unsubstituted."
      )
      missing_players.add(player_id)
    return val   # leave as-is so it's visible as an error

  # -------------------------------------------------------------------------
  # Helper: substitute player names within a concatenated pair string
  # e.g. "FSU 12 Sara FSU 40 AK"  ->  "PLYR-a3f7c2e9 PLYR-b7d1e4f2"
  # Works by replacing each known real_id string within the cell value.
  # -------------------------------------------------------------------------
  def sub_pair_string(val):
    if not isinstance(val, str) or not val.strip():
      return val
    result = val
    for real_id, p_uuid in to_uuid.items():
      if real_id in result:
        result = result.replace(real_id, p_uuid)
    return result

  # -------------------------------------------------------------------------
  # Apply substitutions
  # -------------------------------------------------------------------------

  # Direct player columns (one player name per cell)
  direct_player_cols = [
    'player_a1', 'player_a2', 'player_b1', 'player_b2',
    'serve_player', 'pass_player', 'att_player', 'dig_player'
  ]

  for col in direct_player_cols:
    if col in master_ppr_df.columns:
      master_ppr_df[col] = master_ppr_df[col].apply(sub_player)
    else:
      log_info(f"deidentify_league_ppr: column '{col}' not found in dataframe, skipping")

  # Concatenated pair string columns
  pair_string_cols = ['teama', 'teamb', 'point_outcome_team']

  for col in pair_string_cols:
    if col in master_ppr_df.columns:
      master_ppr_df[col] = master_ppr_df[col].apply(sub_pair_string)
    else:
      log_info(f"deidentify_league_ppr: column '{col}' not found in dataframe, skipping")

  # -------------------------------------------------------------------------
  # Summary log
  # -------------------------------------------------------------------------
  if missing_players:
    log_error(
      f"deidentify_league_ppr: COMPLETE WITH ERRORS — "
      f"{len(missing_players)} unmapped player(s) left in data: {missing_players}"
    )
  else:
    log_info(f"deidentify_league_ppr: COMPLETE — all players successfully substituted")

  return master_ppr_df


# =============================================================================
#
#   create_master_ppr  (callable entry point — launches background task)
#
# =============================================================================

@anvil.server.callable
def create_master_ppr(user_league, user_gender, user_year, user_team, data_set):
  # main idea here is to go thru the datafiles table, limited by league, gender, year, and team.
  # then just append all the different ppr csv files together into a master. Then write this to a csv file
  # then put this into a data table

  # data_set =
  #    Private  - only the team private file
  #    Scouting - the league based scouting file
  #    League   - the league wide file that includes all files for a league (DE-IDENTIFIED)
  #
  # ppr_csv_tables will have a 'Team' designation for each team (i.e. FSU, LSU, Internals),
  # 'Scout' for scouting data, and 'League' for the full league file.

  task = anvil.server.launch_background_task('make_master_ppr', user_league, user_gender, user_year, user_team, data_set)
  return task


@anvil.server.background_task
def make_master_ppr(user_league, user_gender, user_year, user_team, data_set):
  task = make_master_ppr_not_background(user_league, user_gender, user_year, user_team, data_set)
  return task


# =============================================================================
#
#   make_master_ppr_not_background
#
#   Assembles the master ppr dataframe from btd_files, then writes to
#   ppr_csv_tables.
#
#   For data_set == 'League': de-identifies player names before writing.
#
# =============================================================================

@anvil.server.callable
def make_master_ppr_not_background(user_league, user_gender, user_year, user_team, data_set):

  log_info(f"make_master_ppr_not_background: started for {user_league}/{user_gender}/{user_year}, team={user_team}, data_set={data_set}")

  master_ppr_df = pd.DataFrame()

  # --- Select which btd_files rows to include ---
  if data_set == "Private":
    btd_rows = app_tables.btd_files.search(
      league=user_league,
      gender=user_gender,
      year=user_year,
      team=user_team,
      private=True
    )

  elif data_set == "Scouting":
    btd_rows = app_tables.btd_files.search(
      league=user_league,
      gender=user_gender,
      year=user_year,
      private=False
    )
    user_team = "Scout"

  elif data_set == "League":
    btd_rows = app_tables.btd_files.search(
      league=user_league,
      gender=user_gender,
      year=user_year
    )
    user_team = "League"

  else:
    log_error(f"make_master_ppr_not_background: unknown data_set value: '{data_set}'")
    return False

  # --- Assemble master ppr dataframe from individual btd ppr files ---
  files_found = 0
  for flist_r in btd_rows:
    if flist_r['ppr_data']:
      ppr_df = pd.read_csv(io.BytesIO(flist_r['ppr_data'].get_bytes()))
      master_ppr_df = pd.concat([master_ppr_df, ppr_df])
      files_found += 1

  log_info(f"make_master_ppr_not_background: assembled {files_found} ppr files, total rows={master_ppr_df.shape[0]}")

  if master_ppr_df.empty:
    log_info(f"make_master_ppr_not_background: no data found for {user_league}/{user_gender}/{user_year}/{data_set}, nothing to write")
    return True

  # --- De-identify before writing if this is the League file ---
  if data_set == "League":
    log_info("make_master_ppr_not_background: de-identifying League ppr before writing")
    master_ppr_df = deidentify_league_ppr(master_ppr_df, user_league, user_gender, user_year)

  # --- Write to ppr_csv_tables ---
  create_master_ppr_table(master_ppr_df, user_league, user_gender, user_year, user_team)

  log_info(f"make_master_ppr_not_background: complete for {user_league}/{user_gender}/{user_year}/{user_team}")
  return True


# =============================================================================
#
#   create_master_ppr_table
#
#   Cleans up the assembled dataframe and writes it as a CSV to ppr_csv_tables.
#   No changes from original except the dig_player bug fix (line noted below).
#
# =============================================================================

def create_master_ppr_table(master_ppr_df, user_league, user_gender, user_year, user_team):
  # Clean up each row and write the master ppr dataframe to ppr_csv_tables

  for d in master_ppr_df.to_dict(orient="records"):

    d['league'] = d['league'] if isinstance(d['league'], str) else ""
    d['gender'] = d['gender'] if isinstance(d['gender'], str) else ""
    d['year'] = d['year'] if isinstance(d['year'], (float, int)) else 0
    d['comp_l1'] = d['comp_l1'] if isinstance(d['comp_l1'], str) else ""
    d['comp_l2'] = d['comp_l2'] if isinstance(d['comp_l2'], str) else ""
    d['comp_l3'] = d['comp_l3'] if isinstance(d['comp_l3'], str) else ""
    d['team'] = d['point_no'] if isinstance(d['point_no'], (float, int)) else 0
    d['filename'] = d['filename'] if isinstance(d['filename'], str) else ""
    d['video_id'] = d['video_id'] if isinstance(d['video_id'], str) else ""
    d['rally_id'] = d['rally_id'] if isinstance(d['rally_id'], (float, int)) else 0
    d['point_no'] = d['point_no'] if isinstance(d['point_no'], (float, int)) else 0
    d['a_set_diff'] = d['a_set_diff'] if isinstance(d['a_set_diff'], (float, int)) else 0
    d['a_score_diff'] = d['a_score_diff'] if isinstance(d['a_score_diff'], (float, int)) else 0
    d['set'] = d['set'] if isinstance(d['set'], (float, int)) else 0
    d['a_score'] = d['a_score'] if isinstance(d['a_score'], (float, int)) else 0
    d['teama'] = d['teama'] if isinstance(d['teama'], str) else ""
    d['player_a1'] = d['player_a1'] if isinstance(d['player_a1'], str) else ""
    d['player_a2'] = d['player_a2'] if isinstance(d['player_a2'], str) else ""
    d['teamb'] = d['teamb'] if isinstance(d['teamb'], str) else ""
    d['player_b1'] = d['player_b1'] if isinstance(d['player_b1'], str) else ""
    d['player_b2'] = d['player_b2'] if isinstance(d['player_b2'], str) else ""

    d['serve_player'] = d['serve_player'] if isinstance(d['serve_player'], str) else ""
    d['serve_src_x'] = d['serve_src_x'] if isinstance(d['serve_src_x'], (float, int)) else 0
    d['serve_src_y'] = d['serve_src_y'] if isinstance(d['serve_src_y'], (float, int)) else 0
    d['serve_src_t'] = d['serve_src_t'] if isinstance(d['serve_src_t'], (float, int)) else 0
    d['serve_src_zone_depth'] = d['serve_src_zone_depth'] if isinstance(d['serve_src_zone_depth'], str) else ""
    d['serve_src_zone_net'] = d['serve_src_zone_net'] if isinstance(d['serve_src_zone_net'], (float, int)) else 0
    d['serve_dest_x'] = d['serve_dest_x'] if isinstance(d['serve_dest_x'], (float, int)) else 0
    d['serve_dest_y'] = d['serve_dest_y'] if isinstance(d['serve_dest_y'], (float, int)) else 0
    d['serve_dest_zone_depth'] = d['serve_dest_zone_depth'] if isinstance(d['serve_dest_zone_depth'], str) else ""
    d['serve_dest_zone_net'] = d['serve_dest_zone_net'] if isinstance(d['serve_dest_zone_net'], (float, int)) else 0
    d['serve_dist'] = d['serve_dist'] if isinstance(d['serve_dist'], (float, int)) else 0
    d['serve_dur'] = d['serve_dur'] if isinstance(d['serve_dur'], (float, int)) else 0
    d['serve_speed'] = d['serve_speed'] if isinstance(d['serve_speed'], (float, int)) else 0
    d['serve_angle'] = d['serve_angle'] if isinstance(d['serve_angle'], (float, int)) else 0
    d['serve_action_id'] = d['serve_action_id'] if isinstance(d['serve_action_id'], (float, int)) else 0
    d['serve_height'] = d['serve_height'] if isinstance(d['serve_height'], (float, int)) else 0

    d['pass_player'] = d['pass_player'] if isinstance(d['pass_player'], str) else ""
    d['pass_src_x'] = d['pass_src_x'] if isinstance(d['pass_src_x'], (float, int)) else 0
    d['pass_src_y'] = d['pass_src_y'] if isinstance(d['pass_src_y'], (float, int)) else 0
    d['pass_src_t'] = d['pass_src_t'] if isinstance(d['pass_src_t'], (float, int)) else 0
    d['pass_src_zone_depth'] = d['pass_src_zone_depth'] if isinstance(d['pass_src_zone_depth'], str) else ""
    d['pass_src_zone_net'] = d['pass_src_zone_net'] if isinstance(d['pass_src_zone_net'], (float, int)) else 0
    d['pass_dest_x'] = d['pass_dest_x'] if isinstance(d['pass_dest_x'], (float, int)) else 0
    d['pass_dest_y'] = d['pass_dest_y'] if isinstance(d['pass_dest_y'], (float, int)) else 0
    d['pass_dest_zone_depth'] = d['pass_dest_zone_depth'] if isinstance(d['pass_dest_zone_depth'], str) else ""
    d['pass_dest_zone_net'] = d['pass_dest_zone_net'] if isinstance(d['pass_dest_zone_net'], (float, int)) else 0
    d['pass_dist'] = d['pass_dist'] if isinstance(d['pass_dist'], (float, int)) else 0
    d['pass_dur'] = d['pass_dur'] if isinstance(d['pass_dur'], (float, int)) else 0
    d['pass_speed'] = d['pass_speed'] if isinstance(d['pass_speed'], (float, int)) else 0
    d['pass_angle'] = d['pass_angle'] if isinstance(d['pass_angle'], (float, int)) else 0
    d['pass_action_id'] = d['pass_action_id'] if isinstance(d['pass_action_id'], (float, int)) else 0
    d['pass_height'] = d['pass_height'] if isinstance(d['pass_height'], (float, int)) else 0

    d['set_player'] = d['set_player'] if isinstance(d['set_player'], str) else ""
    d['set_src_x'] = d['set_src_x'] if isinstance(d['set_src_x'], (float, int)) else 0
    d['set_src_y'] = d['set_src_y'] if isinstance(d['set_src_y'], (float, int)) else 0
    d['set_src_t'] = d['set_src_t'] if isinstance(d['set_src_t'], (float, int)) else 0
    d['set_src_zone_depth'] = d['set_src_zone_depth'] if isinstance(d['set_src_zone_depth'], str) else ""
    d['set_src_zone_net'] = d['set_src_zone_net'] if isinstance(d['set_src_zone_net'], (float, int)) else 0
    d['set_dest_x'] = d['set_dest_x'] if isinstance(d['set_dest_x'], (float, int)) else 0
    d['set_dest_y'] = d['set_dest_y'] if isinstance(d['set_dest_y'], (float, int)) else 0
    d['set_dest_zone_depth'] = d['set_dest_zone_depth'] if isinstance(d['set_dest_zone_depth'], str) else ""
    d['set_dest_zone_net'] = d['set_dest_zone_net'] if isinstance(d['set_dest_zone_net'], (float, int)) else 0
    d['set_dist'] = d['set_dist'] if isinstance(d['set_dist'], (float, int)) else 0
    d['set_dur'] = d['set_dur'] if isinstance(d['set_dur'], (float, int)) else 0
    d['set_speed'] = d['set_speed'] if isinstance(d['set_speed'], (float, int)) else 0
    d['set_angle'] = d['set_angle'] if isinstance(d['set_angle'], (float, int)) else 0
    d['set_action_id'] = d['set_action_id'] if isinstance(d['set_action_id'], (float, int)) else 0
    d['set_height'] = d['set_height'] if isinstance(d['set_height'], (float, int)) else 0

    d['att_player'] = d['att_player'] if isinstance(d['att_player'], str) else ""
    d['att_src_x'] = d['att_src_x'] if isinstance(d['att_src_x'], (float, int)) else 0
    d['att_src_y'] = d['att_src_y'] if isinstance(d['att_src_y'], (float, int)) else 0
    d['att_src_t'] = d['att_src_t'] if isinstance(d['att_src_t'], (float, int)) else 0
    d['att_src_zone_depth'] = d['att_src_zone_depth'] if isinstance(d['att_src_zone_depth'], str) else ""
    d['att_src_zone_net'] = d['att_src_zone_net'] if isinstance(d['att_src_zone_net'], (float, int)) else 0
    d['att_dest_x'] = d['att_dest_x'] if isinstance(d['att_dest_x'], (float, int)) else 0
    d['att_dest_y'] = d['att_dest_y'] if isinstance(d['att_dest_y'], (float, int)) else 0
    d['att_dest_zone_depth'] = d['att_dest_zone_depth'] if isinstance(d['att_dest_zone_depth'], str) else ""
    d['att_dest_zone_net'] = d['att_dest_zone_net'] if isinstance(d['att_dest_zone_net'], (float, int)) else 0
    d['att_dist'] = d['att_dist'] if isinstance(d['att_dist'], (float, int)) else 0
    d['att_dur'] = d['att_dur'] if isinstance(d['att_dur'], (float, int)) else 0
    d['att_speed'] = d['att_speed'] if isinstance(d['att_speed'], (float, int)) else 0
    d['att_angle'] = d['att_angle'] if isinstance(d['att_angle'], (float, int)) else 0
    d['att_action_id'] = d['att_action_id'] if isinstance(d['att_action_id'], (float, int)) else 0
    d['att_height'] = d['att_height'] if isinstance(d['att_height'], (float, int)) else 0

    # BUG FIX: original line 221 was:
    #   d['dig_player'] = d['dig_player'] if not d['dig_player'] else " "
    # This had the condition backwards — it was blanking out any non-empty dig_player.
    # Corrected to: keep the value if it is a string, otherwise default to empty string.
    d['dig_player'] = d['dig_player'] if isinstance(d['dig_player'], str) else ""

    d['dig_src_x'] = d['dig_src_x'] if isinstance(d['dig_src_x'], (float, int)) else 0
    d['dig_src_y'] = d['dig_src_y'] if isinstance(d['dig_src_y'], (float, int)) else 0
    d['dig_src_t'] = d['dig_src_t'] if isinstance(d['dig_src_t'], (float, int)) else 0
    d['dig_src_zone_depth'] = d['dig_src_zone_depth'] if isinstance(d['dig_src_zone_depth'], str) else ""
    d['dig_src_zone_net'] = d['dig_src_zone_net'] if isinstance(d['dig_src_zone_net'], (float, int)) else 0
    d['dig_dest_x'] = d['dig_dest_x'] if isinstance(d['dig_dest_x'], (float, int)) else 0
    d['dig_dest_y'] = d['dig_dest_y'] if isinstance(d['dig_dest_y'], (float, int)) else 0
    d['dig_dest_zone_depth'] = d['dig_dest_zone_depth'] if isinstance(d['dig_dest_zone_depth'], str) else ""
    d['dig_dest_zone_net'] = d['dig_dest_zone_net'] if isinstance(d['dig_dest_zone_net'], (float, int)) else 0
    d['dig_dist'] = d['dig_dist'] if isinstance(d['dig_dist'], (float, int)) else 0
    d['dig_dur'] = d['dig_dur'] if isinstance(d['dig_dur'], (float, int)) else 0
    d['dig_speed'] = d['dig_speed'] if isinstance(d['dig_speed'], (float, int)) else 0
    d['dig_angle'] = d['dig_angle'] if isinstance(d['dig_angle'], (float, int)) else 0
    d['dig_action_id'] = d['dig_action_id'] if isinstance(d['dig_action_id'], (float, int)) else 0
    d['dig_height'] = d['dig_height'] if isinstance(d['dig_height'], (float, int)) else 0

    d['point_outcome'] = d['point_outcome'] if isinstance(d['point_outcome'], str) else ""
    d['point_outcome_team'] = d['point_outcome_team'] if isinstance(d['point_outcome_team'], str) else ""
    d['tactic'] = d['tactic'] if isinstance(d['tactic'], str) else ""
    d['last_action_id'] = d['last_action_id'] if isinstance(d['last_action_id'], (float, int)) else 0

  # Convert the cleaned dataframe to CSV and store in ppr_csv_tables
  master_ppr_csv_file = pd.DataFrame.to_csv(master_ppr_df)
  ppr_media = anvil.BlobMedia(content_type="text/plain", content=master_ppr_csv_file.encode(), name="ppr_csv.csv")

  # Find the correct row in ppr_csv_tables and update or create
  ppr_csv_row = app_tables.ppr_csv_tables.get(
    q.all_of(
      league=user_league,
      gender=user_gender,
      year=user_year,
      team=user_team
    )
  )

  if ppr_csv_row:
    ppr_csv_row.update(
      league=user_league,
      gender=user_gender,
      year=user_year,
      team=user_team,
      ppr_csv=ppr_media,
      date=datetime.now()
    )
  else:
    app_tables.ppr_csv_tables.add_row(
      league=user_league,
      gender=user_gender,
      year=user_year,
      team=user_team,
      ppr_csv=ppr_media,
      date=datetime.now(),
      min_att=5   # can be changed directly in the Anvil data table
    )