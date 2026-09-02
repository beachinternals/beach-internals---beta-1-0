import anvil.files
from anvil.files import data_files
import anvil.secrets
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import pandas as pd
# ============================================================================
# PERFORMANCE MONITORING IMPORTS
# ============================================================================
from server_functions import (
monitor_performance,
MONITORING_LEVEL_OFF,
MONITORING_LEVEL_CRITICAL,
MONITORING_LEVEL_IMPORTANT,
MONITORING_LEVEL_DETAILED,
MONITORING_LEVEL_VERBOSE
)

# import error logging funcitons
from logger_utils import log_info, log_error, log_critical, log_debug

# ============================================================================
#
#  AUTH HELPER
#  All callables in this file are INTERNALS only — they trigger
#  league-wide data recalculations.
#
# ============================================================================

def _require_internals():
  """
  Verify the caller is logged in AND is on the INTERNALS team.
  Raises Exception if not authorized. Returns user row.
  """
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if user['team'] != 'INTERNALS':
    raise Exception("Access denied: this function is for admins only.")
  return user



from server_functions import *
from datetime import datetime, timedelta, date
from plot_functions import *
from calc_player_data_dictionary import calculate_player_data_via_dictionary

# ########## Calculate league summaries, stored as player data
#
#  player_data is only for league entries in the ppr_csv_tables table.  We also sotre the list of player data stats

@anvil.server.callable
def calc_all_player_data():
  _require_internals()
  # caluclate the plaeyr data for ALL leagues
  # so seach th eleague data base, then loop thru them

  # for each row:
  for l_row in app_tables.ppr_csv_tables.search(team='League'):
    task = calc_player_data_background(l_row['league'],l_row['gender'],l_row['year'])

  return task

# begin with the server callable task, this then provides status and calls the background task
@anvil.server.callable
def calc_player_data_background( c_league, c_gender, c_year):
  _require_internals()
  #
  # calculate the player data files for all teams in the league, gender, year given
  #

  #print(f'Calling Background Task calculate_player_data for {c_league},{c_gender},{c_year}')
  task = anvil.server.launch_background_task('calculate_player_data', c_league, c_gender, c_year)

  # check return status
  #print(f' Background Task, Task id:{task.get_id()} return Status:{task.get_termination_status()}')

  return task

#----------------------------
#
# Define the background task to calculate the player_data for a single league
#
#---------------------------------------------
@anvil.server.background_task
def calculate_player_data( c_league, c_gender, c_year):
  return calculate_player_data_not_background(c_league, c_gender, c_year)

def calculate_player_data_not_background(c_league, c_gender, c_year):
  """
  Computes player_data/player_data_stats for a league via the
  metric_dictionary-driven engine (calc_player_data_dictionary.py).

  This replaced a ~800-line hardcoded per-player calculation on 2026-09-02,
  after validating the dictionary-driven engine against the legacy output
  across 6 players (see metric_dictionary_diff.py, the verification tool
  built for this migration). Confirmed differences from the legacy output
  fall into: intentional scope changes (fbhe1-5/fbhe_oos/fbhe_bang etc. now
  scoped to "player passed AND attacked", not just attacked), confirmed
  legacy bugs (fbhe5's zone filter; fbhe_bang/_n/_per were actually
  fbhe_shoot's numbers due to a variable-reuse bug), and a deliberate
  basis-consistency choice for the tcr/error-density consistency metrics
  (see player_data_column_alias_map.py's SCALE_TRANSFORMS comments).
  """
  result_string = "Calculate Player Data server module Called"

  c_team = "League"    # only updating the league tables
  log_info(f"League:{c_league}, Gender:{c_gender}, Year:{c_year}, Team:{c_team}")

  ppr_csv_row = app_tables.ppr_csv_tables.get(
    q.all_of(
      league = c_league,
      gender = c_gender,
      year = c_year,
      team = c_team
    ) )
  if not ppr_csv_row:
    log_error(f"calc_player_data: ERROR - No ppr_csv_row found for {c_league} {c_gender} {c_year}")
    return ["No Rows"]

  player_df, player_stats_df, meta = calculate_player_data_via_dictionary(c_league, c_gender, c_year)
  if player_df is None:
    log_error(f"calc_player_data: ERROR - {meta.get('error')} for {c_league} {c_gender} {c_year}")
    return ["No Rows"]

  log_info(f"About to save player_data for {c_league} {c_gender} {c_year}")
  log_info(f"player_df shape: {player_df.shape}, player_stats_df shape: {player_stats_df.shape}")

  try:
    player_csv_file = pd.DataFrame.to_csv(player_df)
    player_media = anvil.BlobMedia(content_type="text/plain", content=player_csv_file.encode(), name="player_data.csv")
    log_info(f"Created player_media, size: {len(player_csv_file)} bytes")

    player_stats_csv = pd.DataFrame.to_csv(player_stats_df)
    player_stats_media = anvil.BlobMedia(content_type="text/plain", content=player_stats_csv.encode(), name="player_sats.csv")
    log_info(f"Created player_stats_media, size: {len(player_stats_csv)} bytes")

    log_info(f"Calling ppr_csv_row.update() for {c_league} {c_gender} {c_year}")
    ppr_csv_row.update(
      player_data = player_media,
      player_data_date = datetime.now(),
      player_data_stats = player_stats_media,
      player_data_stats_date = datetime.now()
    )
    log_info(f"Successfully called ppr_csv_row.update() for {c_league} {c_gender} {c_year}")

  except Exception as e:
    log_error(f"ERROR saving player_data: {e}")
    import traceback
    log_error(f"Traceback: {traceback.format_exc()}")
    return f"ERROR: {e}"

  return result_string
