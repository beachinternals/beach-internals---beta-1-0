import anvil.secrets
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from datetime import datetime, timedelta
import pandas as pd
import io
import rebuild_data
import numpy as np

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

# Import other modules
from btd_ppr_conversion import *
from ppr_master_merge import *
from calc_player_data import *
from calc_pair_data import *
from calc_traingle_scoring import *
from s_w_report import *


# ============================================================================
#
#  AUTH HELPER
#  All callable functions in this file are admin/INTERNALS only.
#  Call _require_internals() as the first line of every @anvil.server.callable.
#
# ============================================================================

def _require_internals():
  """
  Verify the caller is logged in AND is on the INTERNALS team.
  Raises a plain Exception (shown to the user) if not authorized.
  Returns the user row in case the caller needs it.
  """
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if user['team'] != 'INTERNALS':
    raise Exception("Access denied: this function is for admins only.")
  return user


# ============================================================================
#
#  Night Processing
#
#  A single night processing routine that runs everything in sequence.
#  This limits processor time (as opposed to multiple background jobs).
#
#  Order:
#    1) btd > ppr                      : call_build_ppr > build_ppr
#    2) Merge ppr files                : call_merge_ppr > merge_ppr
#    3) Calculate data and tri files   : call_calc_player_data > calc_player_data
#    4) create reports
#
# ============================================================================

@anvil.server.callable
def night_processing_callable(c_league, c_gender, c_year, rebuild_all, all_leagues):
  # This one we can call from the browser for testing.
  # all_leagues = True for all leagues
  # rebuild_all = True to rebuild the ppr files for all btd files
  _require_internals()
  return anvil.server.launch_background_task(
    'night_processing_backgound', c_league, c_gender, c_year, rebuild_all, all_leagues
  )

@anvil.server.background_task
def night_processing_cron():
  # Stub called by the cron scheduler — runs server-side, no auth check needed
  return anvil.server.launch_background_task('night_processing_backgound', '', '', '', False, True)


@anvil.server.background_task
@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def night_processing_backgound(d_league, d_gender, d_year, rebuild_all, all_leagues):
  # call all the different night processing tasks in sequence, league by league

  now = datetime.now()
  email_message = 'Night Processing Started at :' + str(now) + "\n"
  email_message += 'All Leagues:' + str(all_leagues) + '.  Rebuild All:' + str(rebuild_all)
  email_message += '   League:' + d_league + '   Gender:' + d_gender + '.  Year:' + d_year + '\n'

  dict = {
    'league': [str()], 'gender': [str()], 'year': [str()],
    'team': [str()], 'update': [str()]
  }
  btd_df = pd.DataFrame.from_records(dict)
  i = 0
  for btd_file_r in app_tables.btd_files.search():
    btd_df.at[i, 'league'] = btd_file_r['league']
    btd_df.at[i, 'gender'] = btd_file_r['gender']
    btd_df.at[i, 'year'] = btd_file_r['year']
    btd_df.at[i, 'team'] = btd_file_r['team']
    btd_df.at[i, 'update'] = 'No'
    i += 1

  league_list = pd.unique(btd_df['league'])
  gender_list = pd.unique(btd_df['gender'])
  year_list = pd.unique(btd_df['year'])
  team_list = pd.unique(btd_df['team'])

  for c_league in league_list:
    for c_gender in gender_list:
      for c_year in year_list:
        new_league_data = False
        new_team_data = False
        if (all_leagues) or ((c_league == d_league) and (c_gender == d_gender) and (c_year == d_year)):
          for c_team in team_list:
            email_message += 'Generating PPR files for: ' + c_league + ' ' + c_gender + ' ' + c_year + ' ' + c_team + '\n'
            r_value, new_team_data = generate_ppr_files_not_background(c_league, c_gender, c_year, c_team, rebuild_all)

            if new_team_data:
              new_league_data = True
              email_message += ' Merging PPR Files for ' + c_league + " " + c_gender + " " + c_year + " " + c_team + "\n"
              r_val = make_master_ppr_not_background(c_league, c_gender, c_year, c_team, 'Private')
              r_val = make_master_ppr_not_background(c_league, c_gender, c_year, c_team, 'Scouting')
              email_message += ' Merging PPR Files for ' + c_league + ' ' + c_gender + ' ' + c_year + ' League' + "\n"
              r_val = make_master_ppr_not_background(c_league, c_gender, c_year, c_team, 'League')

        if new_league_data:
          # Calculate Player Data
          email_message += ' Calculating Player Data for ' + c_league + ' ' + c_gender + ' ' + c_year + "\n"
          r_val = calculate_player_data_not_background(c_league, c_gender, c_year)
          email_message += '        ' + str(r_val) + "\n"

          # Calculate Pair Table (MOVED BEFORE TRIANGLE DATA)
          email_message += ' Building Pair Table for ' + c_league + ' ' + c_gender + ' ' + c_year + "\n"
          r_val = build_pair_df(c_league, c_gender, c_year)
          email_message += '        ' + str(r_val) + "\n"

          # Load Pair Data into master_pair table (MOVED BEFORE TRIANGLE DATA)
          email_message += ' Loading Pair data into master_pair table for ' + c_league + ' ' + c_gender + ' ' + c_year + "\n"
          r_val = load_pair_data_table_for_league(c_league, c_gender, c_year)
          email_message += '        ' + str(r_val) + "\n"

          # Calculate Triangle Data (MOVED AFTER PAIR TABLE IS LOADED)
          email_message += ' Calculating Triangle Data for ' + c_league + ' ' + c_gender + ' ' + c_year + "\n"
          r_val = calculate_triangle_scoring_not_background(c_league, c_gender, c_year)
          email_message += '        ' + str(r_val) + "\n"

          # Calculate Pair Data
          email_message += ' Building Pair Data for ' + c_league + ' ' + c_gender + ' ' + c_year + "\n"
          r_val = calculate_pair_data_not_background(c_league, c_gender, c_year)
          email_message += '        ' + str(r_val) + "\n"

          # Pair Data & Stats
          email_message += ' Building Pair Data & Stats for ' + c_league + ' ' + c_gender + ' ' + c_year + "\n"
          r_val = calc_s_w_player(c_league, c_gender, c_year)
          email_message += '        ' + str(r_val) + "\n"

          # Strengths & Weaknesses
          email_message += ' Building Strengths & Weaknesses for ' + c_league + ' ' + c_gender + ' ' + c_year + "\n"
          r_val = calc_s_w_player(c_league, c_gender, c_year)
          email_message += '        ' + str(r_val) + "\n"
        else:
          email_message += 'No New data Found for ' + c_league + ' ' + c_gender + ' ' + c_year + "\n"

  # The very last thing — load the pair's data table
  email_message += ' Loading Pair data Table \n'
  r_val = load_pair_data_table()
  email_message += '        ' + str(r_val) + "\n"

  internals_email = 'beachinternals@gmail.com'
  now1 = datetime.now()
  email_message += "Night Processing Completed at:" + str(now1) + ' Compute time: ' + str(now1 - now) + "\n"
  anvil.email.send(
    to=internals_email, from_address="no-reply",
    subject='Beach Internals - Night Processing', text=email_message
  )
  return True


# ============================================================================
#
#  Build PPR Data
#  Convert btd to ppr and save in ppr_csv table
#
# ============================================================================

@anvil.server.callable
def call_build_ppr_data():
  """Launch PPR build for new files only. INTERNALS only."""
  _require_internals()
  return anvil.server.launch_background_task('build_ppr_data1')

@anvil.server.callable
def call_build_all_ppr_data(disp_league, disp_gender, disp_year):
  """Launch PPR rebuild for a specific or all league/gender/year. INTERNALS only."""
  _require_internals()
  return anvil.server.launch_background_task('build_ppr_data2', disp_league, disp_gender, disp_year)


@anvil.server.background_task
def build_ppr_data1():
  return calculate_ppr_data(False)

@anvil.server.background_task
def build_ppr_data2(disp_league, disp_gender, disp_year):
  return calculate_ppr_data(True, disp_league, disp_gender, disp_year)


@anvil.server.callable
def calculate_ppr_data(rebuild, disp_league='', disp_gender='', disp_year=''):
  """
  Calculate PPR data from BTD files. INTERNALS only.

  Parameters:
    rebuild      : True = rebuild all ppr files, False = only process new/updated files
    disp_league  : Filter to a specific league (empty string = all leagues)
    disp_gender  : Filter to a specific gender (empty string = all genders)
    disp_year    : Filter to a specific year   (empty string = all years)
  """
  _require_internals()

  now = datetime.now()
  all_leagues = (disp_league == '' and disp_gender == '' and disp_year == '')
  email_text = "Calculate PPR Data \n Called at: " + str(now) + "\n"
  if all_leagues:
    email_text += "Mode: ALL leagues\n"
  else:
    email_text += f"Mode: Single league -> League:{disp_league}  Gender:{disp_gender}  Year:{disp_year}\n"

  dict = {
    'league': [str()], 'gender': [str()],
    'year':   [str()], 'team':   [str()]
  }
  btd_df = pd.DataFrame.from_records(dict)
  i = 0
  for btd_file_r in app_tables.btd_files.search():
    btd_df.at[i, 'league'] = btd_file_r['league']
    btd_df.at[i, 'gender'] = btd_file_r['gender']
    btd_df.at[i, 'year']   = str(btd_file_r['year'])
    btd_df.at[i, 'team']   = btd_file_r['team']
    i += 1

  league_list = pd.unique(btd_df['league'])
  gender_list = pd.unique(btd_df['gender'])
  year_list   = pd.unique(btd_df['year'])
  team_list   = pd.unique(btd_df['team'])
  r_val        = None
  email_status = None

  for c_league in league_list:
    for c_gender in gender_list:
      for c_year in year_list:
        if all_leagues:
          process_this = True
        else:
          process_this = (
            c_league == disp_league and
            c_gender == disp_gender and
            str(c_year) == str(disp_year)
          )
        if not process_this:
          continue

        for c_team in team_list:
          email_text += f"  Generating PPR Files for: {c_league} {c_gender} {c_year} {c_team}\n"
          r_val = anvil.server.launch_background_task(
            'generate_ppr_files', c_league, c_gender, c_year, c_team, rebuild
          )
        # NOTE: League merged table is built separately via calculate_league_ppr()
        # Run that AFTER this function completes to avoid timing issues.

  internals_email = 'beachinternals@gmail.com'
  now_end = datetime.now()
  email_text += f"Completed at: {str(now_end)}  (compute time: {str(now_end - now)})\n"
  email_status = anvil.email.send(
    to=internals_email, from_address="no-reply",
    subject='Beach Internals - Rebuild PPR Data', text=email_text
  )
  return r_val, email_status


# ============================================================================
#
#  Merge PPR Data
#
# ============================================================================

@anvil.server.callable
def call_merge_ppr_data():
  """Launch PPR merge as background task. INTERNALS only."""
  _require_internals()
  return anvil.server.launch_background_task('merge_ppr_data1')

@anvil.server.background_task
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def merge_ppr_data1():
  return merge_ppr_data()

@anvil.server.callable
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def merge_ppr_data():
  """Merge all PPR files across all leagues/teams. INTERNALS only."""
  _require_internals()

  now = datetime.now()
  email_text = "Merging PPR Data \n Called at:" + str(now) + "\n"

  dict = {'league': [str()], 'gender': [str()], 'year': [str()], 'team': [str()]}
  btd_df = pd.DataFrame.from_records(dict)
  i = 0
  for btd_file_r in app_tables.btd_files.search():
    btd_df.at[i, 'league'] = btd_file_r['league']
    btd_df.at[i, 'gender'] = btd_file_r['gender']
    btd_df.at[i, 'year'] = btd_file_r['year']
    btd_df.at[i, 'team'] = btd_file_r['team']
    i += 1

  league_list = pd.unique(btd_df['league'])
  gender_list = pd.unique(btd_df['gender'])
  year_list = pd.unique(btd_df['year'])
  team_list = pd.unique(btd_df['team'])

  r_val = None
  for c_league in league_list:
    for c_gender in gender_list:
      for c_year in year_list:
        for c_team in team_list:
          email_text += ' Merging PPR Files for' + c_league + " " + c_gender + " " + c_year + " " + c_team + "\n"
          print(email_text)
          r_val = anvil.server.launch_background_task('make_master_ppr', c_league, c_gender, c_year, c_team, 'Private')
          r_val = anvil.server.launch_background_task('make_master_ppr', c_league, c_gender, c_year, c_team, 'Scouting')
          if c_team == 'INTERNALS':
            email_text += ' Merging PPR Files for' + ' ' + c_league + " " + c_gender + " " + c_year + " " + c_team + ' League' + "\n"
            r_val = anvil.server.launch_background_task('make_master_ppr', c_league, c_gender, c_year, c_team, 'League')

  internals_email = 'beachinternals@gmail.com'
  email_text += "Completed at:" + str(now) + "\n"
  email_status = anvil.email.send(
    to=internals_email, from_address="no-reply",
    subject='Beach Internals - Merge PPR Data', text=email_text
  )
  return r_val, email_status


# ============================================================================
#
#  Calculate Player / Triangle / Pair Data
#
# ============================================================================

@anvil.server.callable
def call_calculate_data():
  """Launch all downstream calculations as background task. INTERNALS only."""
  _require_internals()
  return anvil.server.launch_background_task('calculate_data')

@anvil.server.background_task
def calculate_data():
  return calculate_data1()

@anvil.server.callable
def calculate_data1():
  """Run player data, triangle data, and pair table calcs for all leagues. INTERNALS only."""
  _require_internals()

  now = datetime.now()
  email_text = "Calculate PPR Data \n Called at:" + str(now) + "\n"

  dict = {'league': [str()], 'gender': [str()], 'year': [str()], 'team': [str()]}
  btd_df = pd.DataFrame.from_records(dict)
  i = 0
  for btd_file_r in app_tables.btd_files.search():
    btd_df.at[i, 'league'] = btd_file_r['league']
    btd_df.at[i, 'gender'] = btd_file_r['gender']
    btd_df.at[i, 'year'] = btd_file_r['year']
    i += 1

  league_list = pd.unique(btd_df['league'])
  gender_list = pd.unique(btd_df['gender'])
  year_list = pd.unique(btd_df['year'])

  r_val = None
  for c_league in league_list:
    for c_gender in gender_list:
      for c_year in year_list:
        email_text += ' Calculating Player Data for ' + c_league + ' ' + c_gender + ' ' + c_year + "\n"
        r_val = calculate_player_data(c_league, c_gender, c_year)
        email_text += ' Calculating Triangle Data for ' + c_league + ' ' + c_gender + ' ' + c_year + "\n"
        r_val = calculate_triangle_scoring(c_league, c_gender, c_year)
        email_text += ' Building Pair Table for ' + c_league + ' ' + c_gender + ' ' + c_year + "\n"
        r_val = build_pair_table_background(c_league, c_gender, c_year)

  internals_email = 'beachinternals@gmail.com'
  email_text += "Completed at:" + str(now) + "\n"
  email_status = anvil.email.send(
    to=internals_email, from_address="no-reply",
    subject='Beach Internals - Calculate player and Triangle Data', text=email_text
  )
  return r_val, email_status


@anvil.server.callable
def build_pair_table(c_league, c_gender, c_year):
  """Build pair table for a specific league. INTERNALS only."""
  _require_internals()
  return anvil.server.launch_background_task('build_pair_table_background', c_league, c_gender, c_year)

@anvil.server.callable
def build_pair_data_table():
  """Rebuild the full pair data table. INTERNALS only."""
  _require_internals()
  return anvil.server.launch_background_task('build_pair_data_background')


@anvil.server.background_task
def build_pair_table_background(c_league, c_gender, c_year):
  return build_pair_df(c_league, c_gender, c_year)

def build_pair_df(c_league, c_gender, c_year):
  """
  Internal helper — builds the pair table from the league PPR file.
  Not callable from the browser.
  """
  c_team = "League"
  print(f"League:{c_league}, Gender:{c_gender}, Year:{c_year}, Team:{c_team}")
  ppr_csv_row = app_tables.ppr_csv_tables.get(
    q.all_of(league=c_league, gender=c_gender, year=c_year, team=c_team)
  )

  if ppr_csv_row:
    ppr_df = pd.read_csv(io.BytesIO(ppr_csv_row['ppr_csv'].get_bytes()))
    if ppr_df.shape[0] == 0:
      return ["No Rows"]
  else:
    return ["No Rows"]

  team_list_a = ppr_df[['teama', 'player_a1', 'player_a2']]
  team_list_a = team_list_a.rename(columns={'teama': 'team', 'player_a1': 'player1', 'player_a2': 'player2'})

  team_list_b = ppr_df[['teamb', 'player_b1', 'player_b2']]
  team_list_b = team_list_b.rename(columns={'teamb': 'team', 'player_b1': 'player1', 'player_b2': 'player2'})

  team_list = pd.concat([team_list_a, team_list_b])
  team_list = team_list.drop_duplicates()
  team_list = team_list.sort_values(by=['team'])

  tmp = pd.DataFrame(team_list)
  pair_csv_file = pd.DataFrame.to_csv(tmp)
  print(f"Pair list being updated for: {c_league}, {c_gender}, {c_year}")
  pair_media = anvil.BlobMedia(
    content_type="text/plain", content=pair_csv_file.encode(), name="pair_table.csv"
  )
  ppr_csv_row.update(pair_list=pair_media, pair_list_date=datetime.now())
  return True


@anvil.server.callable
def load_pair_data_table_not_background():
  """Load pair data into the master_pair table (all leagues). INTERNALS only."""
  _require_internals()
  return anvil.server.launch_background_task('build_pair_data_background')

@anvil.server.background_task
def build_pair_data_background():
  return load_pair_data_table()

def load_pair_data_table():
  """
  Internal helper — loads pair data from ppr_csv_tables into master_pair (all leagues).
  Not callable from the browser.
  """
  for lrow in app_tables.ppr_csv_tables.search(team=q.like("League")):
    show_print = True if (lrow['league'] == 'AVP') else False
    print(f"Processing : {lrow['league']}, {lrow['gender']},{lrow['year']}")
    if show_print:
      print(f"pair list : {type(lrow['pair_list'])}")
      print(f"pair list : {lrow['pair_list']}")
    if lrow['pair_list']:
      pair_df = pd.read_csv(io.BytesIO(lrow['pair_list'].get_bytes()))
      if pair_df.shape[0] == 0:
        if show_print:
          print(f"load_pair_data_table: Pair List Df Empty : {lrow['league']}, {lrow['gender']},{lrow['year']}")
        return ["No Pair List Found"]

      for index, p in pair_df.iterrows():
        team_delim = p[1].find(' ')
        if team_delim == -1:
          if show_print:
            print(f"Load Pair Table, no space found looking for Team: {p[1]}")
          a = b  # intentional crash — preserve original behavior
        pair_team = p[1][:team_delim].strip()
        if show_print:
          print(f"load_pair_data_table: Looking for:{lrow['league']}, {lrow['gender']},{lrow['year']} "
                f"p0 Index: {p[0]}, Pair: {p[1]}, Player1: {p[2]}, Player2: {p[3]}, Team: {pair_team}")
        if not app_tables.master_pair.get(
          league=lrow['league'], gender=lrow['gender'], year=lrow['year'],
          player1=p[2], player2=p[3], pair=p[1], team=pair_team
        ):
          if show_print:
            print(f"load_pair_data_table: Adding to master pair list: {lrow['league']}, {lrow['gender']},{lrow['year']} "
                  f"p0 Index: {p[0]}, Team: {p[1]}, Player1: {p[2]}, Player2: {p[3]}")
          app_tables.master_pair.add_row(
            league=lrow['league'], gender=lrow['gender'], year=lrow['year'],
            player1=p[2], player2=p[3], pair=p[1], team=pair_team
          )
    else:
      print(f"No Pair List Data Frame Found : {lrow['league']}, {lrow['gender']},{lrow['year']}")

  return True


def load_pair_data_table_for_league(c_league, c_gender, c_year):
  """
  Load pair data into master_pair table for a specific league/gender/year.
  Called during night processing for each league individually,
  BEFORE calculate_triangle_scoring (which needs pairs already in the table).
  Internal helper — not callable from the browser.
  """
  lrow = app_tables.ppr_csv_tables.get(
    league=c_league, gender=c_gender, year=c_year, team=q.like("League")
  )

  if not lrow:
    print(f"load_pair_data_table_for_league: No league row found for {c_league} {c_gender} {c_year}")
    return ["No League Row Found"]

  show_print = (c_league == 'AVP')
  print(f"load_pair_data_table_for_league: Processing {c_league}, {c_gender}, {c_year}")

  if not lrow['pair_list']:
    print(f"load_pair_data_table_for_league: No Pair List Data for {c_league}, {c_gender}, {c_year}")
    return ["No Pair List Found"]

  pair_df = pd.read_csv(io.BytesIO(lrow['pair_list'].get_bytes()))

  if pair_df.shape[0] == 0:
    print(f"load_pair_data_table_for_league: Pair List Df Empty for {c_league}, {c_gender}, {c_year}")
    return ["No Pair List Found"]

  pairs_added = 0
  for index, p in pair_df.iterrows():
    team_delim = p[1].find(' ')
    if team_delim == -1:
      if show_print:
        print(f"load_pair_data_table_for_league: No space found looking for Team in: {p[1]}")
      continue

    pair_team = p[1][:team_delim].strip()

    if not app_tables.master_pair.get(
      league=c_league, gender=c_gender, year=c_year,
      player1=p[2], player2=p[3], pair=p[1], team=pair_team
    ):
      if show_print:
        print(f"load_pair_data_table_for_league: Adding pair: {p[1]}, Player1: {p[2]}, Player2: {p[3]}")
      app_tables.master_pair.add_row(
        league=c_league, gender=c_gender, year=c_year,
        player1=p[2], player2=p[3], pair=p[1], team=pair_team
      )
      pairs_added += 1

  print(f"load_pair_data_table_for_league: Added {pairs_added} pairs for {c_league} {c_gender} {c_year}")
  return True


# ============================================================================
#
#  Nightly Data Consistency Check
#
# ============================================================================

@anvil.server.background_task
def check_inconsistent_data():
  """
  Nightly background task — checks btd_files and master_player for inconsistencies
  and emails a report to beachinternals@gmail.com.
  Runs server-side via scheduler — no auth check needed here.

  UPDATED: Filters to only check data for the current year (set below).
  """
  try:
    # SET THE YEAR TO CHECK HERE - Change this value to check a different year
    current_year = '2026'

    log_info("Starting nightly data consistency check at %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    issues = []

    # Check btd_files for per_xy == 0, points < 25, errors > 20, duplicate video_id
    # Filtered for current_year only
    btd_issues = []
    btd_rows = list(app_tables.btd_files.search(year=current_year))

    for row in btd_rows:
      row_issues = []
      if row['per_xy'] == 0:
        row_issues.append("per_xy is 0")
      if row['points'] is not None and row['points'] < 25:
        row_issues.append(f"points ({row['points']}) < 25")
      if row['no_errors'] > 20:
        row_issues.append(f"errors > 20 {row['error_str']}")
      if row_issues:
        league = str(row['league'] or 'None')
        gender = str(row['gender'] or 'None')
        year = str(row['year'] if row['year'] is not None else 'None')
        team = str(row['team'] or 'None')
        filename = str(row['filename'] or 'None')
        btd_issues.append(
          f"Row ID {row.get_id()}: {', '.join(row_issues)} "
          f"(league={league}, gender={gender}, year={year}, team={team}, filename={filename})"
        )

    video_id_counts = {}
    for row in btd_rows:
      video_id = row['video_id']
      if video_id is not None:
        video_id = str(video_id)
        video_id_counts[video_id] = video_id_counts.get(video_id, []) + [row]

    for video_id, rows in video_id_counts.items():
      if len(rows) > 1:
        row_ids = [row.get_id() for row in rows]
        for row in rows:
          league = str(row['league'] or 'None')
          gender = str(row['gender'] or 'None')
          year = str(row['year'] if row['year'] is not None else 'None')
          team = str(row['team'] or 'None')
          filename = str(row['filename'] or 'None')
          btd_issues.append(
            f"Row ID {row.get_id()}: Duplicate video_id '{video_id}' (found in Row IDs {', '.join(row_ids)}) "
            f"(league={league}, gender={gender}, year={year}, team={team}, filename={filename})"
          )

    if btd_issues:
      issues.append("<h3>btd_files Issues</h3><ul>" + "".join(f"<li>{issue}</li>" for issue in btd_issues) + "</ul>")

    # Check master_player for single-digit numbers and near-duplicates (NCAA only)
    # Filtered for current_year only
    mp_issues = []
    mp_rows = list(app_tables.master_player.search(year=current_year))
    groups = {}
    for row in mp_rows:
      key = (
        str(row['league'] or 'None'),
        str(row['gender'] or 'None'),
        row['year'] if row['year'] is not None else -1,
        str(row['team'] or 'None')
      )
      groups.setdefault(key, []).append(row)

    for key, rows in groups.items():
      league, gender, year, team = key
      for row in rows:
        number = row['number']
        row_id = row.get_id()
        if number and number.isdigit() and len(number) == 1:
          mp_issues.append(
            f"Row ID {row_id}: Single-digit number '{number}' without leading zero "
            f"(league={league}, gender={gender}, year={year}, team={team})"
          )
        if league == 'NCAA':
          for other_row in rows:
            if row is other_row:
              continue
            other_number = other_row['number']
            if number and other_number and number.lstrip('0') == other_number.lstrip('0'):
              mp_issues.append(
                f"Row ID {row_id} and {other_row.get_id()}: Near-duplicate numbers '{number}' and '{other_number}' "
                f"(league={league}, gender={gender}, year={year}, team={team})"
              )

    if mp_issues:
      issues.append("<h3>master_player Issues</h3><ul>" + "".join(f"<li>{issue}</li>" for issue in mp_issues) + "</ul>")

    email_body = (
      f"<h2>Nightly Data Consistency Report - {datetime.now().strftime('%Y-%m-%d')} ({current_year} Data Only)</h2>"
      f"<p>Found the following inconsistencies in the database:</p>{''.join(issues)}"
      f"<p>Please review these issues and update the data as needed.</p>"
    ) if issues else (
      f"<h2>Nightly Data Consistency Report - {datetime.now().strftime('%Y-%m-%d')} ({current_year} Data Only)</h2>"
      f"<p>No inconsistencies found in btd_files or master_player tables for year {current_year}.</p>"
    )

    anvil.email.send(
      to="beachinternals@gmail.com",
      subject=f"Data Consistency Report - {datetime.now().strftime('%Y-%m-%d')}",
      html=email_body
    )
    log_info("Data consistency check completed and email sent")

  except Exception as e:
    log_error(f"Error in check_inconsistent_data: {str(e)}")
    anvil.email.send(
      to="beachinternals@gmail.com",
      subject=f"Data Consistency Check Error - {datetime.now().strftime('%Y-%m-%d')}",
      html=f"<h2>Error in Nightly Data Consistency Check</h2><p>An error occurred: {str(e)}</p>"
    )


@anvil.server.callable
def trigger_nightly_check():
  """Launch the nightly consistency check manually. INTERNALS only."""
  _require_internals()
  anvil.server.launch_background_task('check_inconsistent_data')
  return {"status": "Nightly check triggered"}


# ============================================================================
#
#  League PPR Table Builder
#
# ============================================================================

@anvil.server.callable
def calc_league_ppr(disp_league='', disp_gender='', disp_year=''):
  """Build the League merged PPR table for each league/gender/year. INTERNALS only."""
  _require_internals()
  anvil.server.launch_background_task('calculate_league_ppr', disp_league, disp_gender, disp_year)
  return {"status": "Calculate League PPR Triggered"}

@anvil.server.background_task
def calculate_league_ppr(disp_league='', disp_gender='', disp_year=''):
  """
  Build the 'League' merged PPR table for each league/gender/year combination.
  Run this AFTER calculate_ppr_data() has fully completed.

  For each unique league/gender/year found in btd_files, calls
  make_master_ppr with data_set='League', which concatenates all teams'
  already-computed PPR files into one combined table in ppr_csv_tables
  with team='League'.

  Parameters:
    disp_league : Filter to a specific league (empty = all)
    disp_gender : Filter to a specific gender (empty = all)
    disp_year   : Filter to a specific year   (empty = all)
  """
  now = datetime.now()
  all_leagues = (disp_league == '' and disp_gender == '' and disp_year == '')
  email_text = "Calculate League PPR Tables\nCalled at: " + str(now) + "\n"
  if all_leagues:
    email_text += "Mode: ALL leagues\n"
  else:
    email_text += f"Mode: {disp_league} {disp_gender} {disp_year}\n"

  seen = set()
  combinations = []
  for btd_file_r in app_tables.btd_files.search():
    key = (btd_file_r['league'], btd_file_r['gender'], str(btd_file_r['year']))
    if key not in seen:
      seen.add(key)
      combinations.append(key)

  r_val = None
  for (c_league, c_gender, c_year) in combinations:
    if not all_leagues:
      if (c_league != disp_league or
          c_gender != disp_gender or
          str(c_year) != str(disp_year)):
        continue

    email_text += f"  Building League table: {c_league} {c_gender} {c_year}\n"
    r_val = anvil.server.launch_background_task(
      'make_master_ppr', c_league, c_gender, c_year, c_league, 'League'
    )

  internals_email = 'beachinternals@gmail.com'
  now_end = datetime.now()
  email_text += f"Completed at: {str(now_end)}  (compute time: {str(now_end - now)})\n"
  anvil.email.send(
    to=internals_email, from_address="no-reply",
    subject='Beach Internals - Build League PPR Tables', text=email_text
  )
  return r_val
