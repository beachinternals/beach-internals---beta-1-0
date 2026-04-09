import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from datetime import datetime, timedelta, date
import json
import traceback
import pandas as pd
import io
import re

# ============================================================================
# LOGGING IMPORTS
# ============================================================================
from .logger_utils import log_debug, log_info, log_error, log_critical

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

# ============================================================================
# EXISTING METRIC GENERATION IMPORT
# ============================================================================
from generate_player_metrics_json_server import generate_player_metrics_json
from generate_player_metrics_markdown import generate_player_metrics_markdown, generate_global_context_markdown
from server_functions import write_to_nested_folder, write_to_nested_folder_with_sharing

from generate_set_level_metrics import (
generate_set_level_metrics_for_player,
format_set_level_data_as_markdown
)

# ============================================================================
# NEW: MULTI-DATASET SUPPORT IMPORTS
# ============================================================================
from ai_export_dataset_combiner import (
get_enabled_datasets,
generate_combined_player_export
)

from generate_set_level_metrics import (
generate_set_level_metrics_for_player
)

# ============================================================================
#
#  AUTH HELPERS
#  _require_internals()    — INTERNALS team only (batch/admin operations)
#  _require_own_team(team) — logged-in AND requesting own team's data
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

def _require_own_team(team):
  """
  Verify the caller is logged in AND is either on the INTERNALS team
  (can access any team) or requesting their own team's data only.
  Returns the user row or raises Exception.
  """
  user = anvil.users.get_user()
  if not user:
    raise Exception("Please log in to continue.")
  if user['team'] != 'INTERNALS' and team != user['team']:
    raise Exception("Access denied: you can only access your own team's data.")
  return user


# ============================================================================
#
#  INPUT VALIDATION HELPER
#
#  _validate_league_params() — validates league, gender, team against live data
#  _validate_lgy_string()    — parses and validates "NCAA | W | 2025" strings
#
#  Leagues are read dynamically from app_tables.league_list, so adding a new
#  league to that table is all that's needed — no code change required here.
#
#  Team is validated against app_tables.subscriptions. INTERNALS is always
#  allowed regardless of subscription records.
#
# ============================================================================

VALID_GENDERS = {'M', 'W'}

def _get_valid_leagues():
  """Return the set of valid league names from the league_list table."""
  try:
    return {row['league'] for row in app_tables.league_list.search()}
  except Exception:
    # If table is unreachable, fail open rather than block all requests
    return None

def _get_valid_teams():
  """Return the set of valid team names from the subscriptions table."""
  try:
    return {row['team'] for row in app_tables.subscriptions.search()}
  except Exception:
    return None

def _validate_league_params(league=None, gender=None, team=None):
  """
  Validate user-supplied league, gender, and/or team parameters.
  Year is intentionally not validated here.
  Raises ValueError with a clear message if any value is invalid.

  Usage:
    _validate_league_params(league=league, gender=gender, team=team)
  """
  if league is not None:
    if not isinstance(league, str) or not league.strip():
      raise ValueError("League must be a non-empty string.")
    valid_leagues = _get_valid_leagues()
    if valid_leagues is not None and league.strip() not in valid_leagues:
      raise ValueError(f"Invalid league: {repr(league)}. Must be one of {sorted(valid_leagues)}.")

  if gender is not None:
    if not isinstance(gender, str) or gender.strip().upper() not in VALID_GENDERS:
      raise ValueError(f"Invalid gender: {repr(gender)}. Must be one of {sorted(VALID_GENDERS)}.")

  if team is not None:
    if not isinstance(team, str) or not team.strip():
      raise ValueError("Team must be a non-empty string.")
    if re.search(r'[<>]', team):
      raise ValueError(f"Invalid team name: {repr(team)}.")
    # INTERNALS can access any team — skip subscription check
    if team.strip() != 'INTERNALS':
      valid_teams = _get_valid_teams()
      if valid_teams is not None and team.strip() not in valid_teams:
        raise ValueError(f"Invalid team: {repr(team)}. Team is not registered in the system.")


def _validate_lgy_string(lgy):
  """
  Parse and validate an lgy string in format "NCAA | W | 2025" or "NCAA|W|2025".
  Validates league and gender. Year is not validated.
  Raises ValueError if format or values are invalid.
  Returns (league, gender, year_str) as cleaned values.
  """
  if not isinstance(lgy, str) or '|' not in lgy:
    raise ValueError(
      f"Invalid league/gender/year format: {repr(lgy)}. Expected 'LEAGUE | GENDER | YEAR'."
    )
  parts = [p.strip() for p in lgy.split('|')]
  if len(parts) != 3:
    raise ValueError(
      f"Invalid league/gender/year format: {repr(lgy)}. Expected exactly 3 parts separated by '|'."
    )
  league, gender, year_str = parts
  _validate_league_params(league=league, gender=gender)
  return league, gender, year_str



# This is a server module for generating NotebookLM-ready markdown files
# with player performance data in JSON format
# Uses ai_export_mgr table for control (similar to rpt_mgr table for reports)

#--------------------------------------------------------------
# Helper function to get PPR data without user check
#--------------------------------------------------------------
def get_filtered_ppr_data_direct(league, gender, year, team, **filters):
  """
    Get PPR data directly using team parameter, bypassing user check.
    This is for scheduled/background tasks where no user is logged in.
    """
  try:
    # Determine search_team (same logic as original function)
    # For INTERNALS, use 'League', otherwise use the team as-is
    if team == 'INTERNALS':
      search_team = 'League'
      log_info(f"INTERNALS team - using search_team='League'")
    else:
      search_team = team
      log_info(f"Using search_team='{search_team}'")

    log_info(f"Querying PPR data for {league}/{gender}/{year}/team={search_team}...")

    # Query ppr_csv_tables directly
    ppr_rows = list(app_tables.ppr_csv_tables.search(
      league=league,
      gender=gender,
      year=year,
      team=search_team
    ))

    if len(ppr_rows) == 0:
      log_error(f"No PPR data found for {league}/{gender}/{year}/team={search_team}")
      return pd.DataFrame()

    log_info(f"Found {len(ppr_rows)} PPR data record(s)")

    # Get the first row
    ppr_row = ppr_rows[0]

    # Load the CSV data
    ppr_csv_data = ppr_row['ppr_csv']

    if hasattr(ppr_csv_data, 'get_bytes'):
      ppr_csv_string = ppr_csv_data.get_bytes().decode('utf-8')
      log_debug("Loaded ppr_csv from Media object")
    else:
      ppr_csv_string = ppr_csv_data
      log_debug("Loaded ppr_csv as string")

    ppr_df = pd.read_csv(io.StringIO(ppr_csv_string))
    log_info(f"Loaded {len(ppr_df)} raw points from PPR")

    # Apply filters using the existing filter function
    from server_functions import filter_ppr_df
    log_info("Applying filters...")
    ppr_df = filter_ppr_df(ppr_df, **filters)
    log_info(f"After filtering: {len(ppr_df)} points retained")

    return ppr_df

  except Exception as e:
    log_exception('error', f"Error in get_filtered_ppr_data_direct", e)
    return pd.DataFrame()

#--------------------------------------------------------------
# Helper function for logging errors with tracebacks
#--------------------------------------------------------------
def log_exception(level, message, exception):
  """Helper to log exceptions with traceback"""
  tb = traceback.format_exc()
  full_message = f"{message}: {str(exception)}\nTraceback:\n{tb}"

  if level == 'critical':
    log_critical(full_message)
  elif level == 'error':
    log_error(full_message)
  elif level == 'debug':
    log_debug(full_message)
  else:
    log_info(full_message)

#--------------------------------------------------------------
# Main function - processes all rows in ai_export_mgr table
#--------------------------------------------------------------
@anvil.server.callable
@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def ai_export_mgr_generate():
  """
    Process all pending exports in the ai_export_mgr table.
    Similar to rpt_mgr_generate() in your report manager.
    
    Returns:
        True when complete
    """
  _require_internals()
  log_info("AI Export Manager - Generate Called")
  print("Starting AI Export Manager...")
  now = datetime.now()
  email_text = f"AI Export Manager Started at: {str(now)}\n\n"

  # Capture the current user to use as a template
  # We'll override the 'team' field with the team from each export request
  current_user = anvil.users.get_user()

  # Launch as background task with user template
  task = anvil.server.launch_background_task('ai_export_mgr_generate_background', current_user)
  log_info("AI Export Manager - Background task launched")

  return True

"""
PATCH for ai_export_manager.py
===============================
Replace the ai_export_mgr_generate_background() function with this version.

Fixes:
  1. 'league referenced before assignment' — force_index used league/team
     variables that only exist inside the loop. Guarded with a check.
  2. Added logging to explain why 0 rows are found (disabled flag status).
"""


@anvil.server.background_task
@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def ai_export_mgr_generate_background(user=None):
  """
  Background task that processes the ai_export_mgr table.

  Args:
      user: The user who initiated the export (for accessing their team's data)
  """
  log_info("AI Export Manager Background - Started")
  if user:
    log_info(f"Running as user: {user['email']}")
  else:
    log_error("No user context provided to background task!")

  now = datetime.now()
  email_text = f"AI Export Manager Started at: {str(now)}\n\n"

  # Track last processed league/team for force_index (only set if loop runs)
  last_league = None
  last_team = None

  try:
    # -------------------------------------------------------
    # Get all rows from ai_export_mgr table
    # -------------------------------------------------------
    all_export_rows = list(app_tables.ai_export_mgr.search(
      tables.order_by('created_at', ascending=True)
    ))

    # Log the breakdown so we can see WHY we might get 0 rows
    total_all = len(all_export_rows)
    total_disabled = sum(1 for row in all_export_rows if row['disabled'] == True)
    total_enabled = sum(1 for row in all_export_rows if row['disabled'] != True)

    log_info(f"ai_export_mgr table: {total_all} total rows, "
             f"{total_disabled} disabled, {total_enabled} enabled (will process)")

    if total_all == 0:
      log_info("Table is empty — nothing to process.")
      log_info("To run an export: add a row to ai_export_mgr with disabled=False")
    elif total_enabled == 0:
      log_info("All rows are disabled — nothing to process.")
      log_info("To re-run: uncheck 'disabled' on the row(s) you want to re-process")

    # Filter to enabled rows only
    export_rows = [row for row in all_export_rows if row['disabled'] != True]

    total_rows = len(export_rows)
    email_text += f"Found {total_rows} enabled export requests\n\n"
    log_info(f"Processing {total_rows} export requests")

    # -------------------------------------------------------
    # Process each enabled row
    # -------------------------------------------------------
    for idx, export_row in enumerate(export_rows, 1):
      try:
        log_info(f"AI Export Manager - Processing export {idx} of {total_rows}")
        email_text += f"\n{'='*60}\n"
        email_text += f"Processing export {idx} of {total_rows}\n"
        email_text += f"{'='*60}\n"

        # -------------------------------------------------------
        # CHECK DAY OF WEEK - skip if not scheduled for today
        # -------------------------------------------------------
        today = datetime.now()
        day_of_week = today.strftime("%A")  # e.g. "Monday", "Tuesday"
        export_dow = export_row['dow'] or 'Everyday'
        if export_dow not in [day_of_week, 'Everyday']:
          log_info(f"SKIPPING: Export for {export_row['team']} not scheduled for {day_of_week} (set to {export_dow})")
          email_text += f"Skipped - not scheduled for {day_of_week}\n"
          continue
          # -------------------------------------------------------

        # Extract parameters from the row
        team = export_row['team']
        export_type = export_row['export_type'] or 'full'

        # Read datasets_included from the row
        datasets_included_rows = export_row['datasets_included']
        if datasets_included_rows:
          datasets_included = list(datasets_included_rows)  # convert Anvil iterator to list
          log_info(f"datasets_included: {len(datasets_included)} datasets")
        else:
          datasets_included = []
          log_info("No datasets_included - will use single aggregate fallback")

        user_email = export_row['user_email']

        log_info(f"Export row data: team={team}, export_type={export_type}")

        # Handle player_filter as linked rows from master player table
        player_filter_rows = export_row['player_filter']
        player_filter = None
        league = export_row['league']  # May be None - we'll derive it

        log_info(f"Player filter rows type: {type(player_filter_rows)}, "
                 f"count: {len(player_filter_rows) if player_filter_rows else 0}")

        if player_filter_rows and len(player_filter_rows) > 0:
          player_filter = []
          leagues_found = set()
          player_data_map = {}

          for player_row in player_filter_rows:
            try:
              team_val = player_row['team']
              number_val = player_row['number']
              shortname_val = player_row['shortname']
              player_name = f"{team_val} {number_val} {shortname_val}"

              league_val = player_row['league']
              gender_val = player_row['gender']
              year_val = player_row['year']
              league_value = f"{league_val} | {gender_val} | {year_val}"

              log_info(f"Built player: {player_name}, league_value: {league_value}")

              player_data_map[player_name] = {
                'team': team_val,
                'number': number_val,
                'shortname': shortname_val,
                'league': league_val,
                'gender': gender_val,
                'year': year_val,
                'league_value': league_value
              }

            except (KeyError, AttributeError) as e:
              log_error(f"Error building player data from row: {e}")
              continue

            if player_name:
              player_filter.append(player_name)
            if league_val:
              leagues_found.add(league_val)

          if len(leagues_found) == 0:
            raise ValueError("No league found for selected players")
          elif len(leagues_found) > 1:
            raise ValueError(
              f"Players from multiple leagues selected: {leagues_found}. "
              f"All players must be from the same league."
            )

          league = list(leagues_found)[0]
          log_info(f"League derived from players: {league}")
          email_text += f"League (derived from players): {league}\n"

        else:
          # No player filter - league must be specified in the table row
          if not league:
            raise ValueError("Either league or player_filter must be specified")
          log_info(f"League from table: {league}")
          email_text += f"League (from table): {league}\n"
          player_data_map = {}

        email_text += f"Team: {team}\n"
        email_text += f"Export Type: {export_type}\n"
        if player_filter:
          email_text += f"Players: {', '.join(player_filter)}\n"
          log_info(f"Players: {', '.join(player_filter)}")
        else:
          email_text += f"Players: All players on team\n"
          log_info("Players: All players on team")

        # Mark as processing
        export_row['started_at'] = datetime.now()
        export_row['league'] = league  # Store derived league back to row

        # NEW: Read de_identified flag (defaults to False if column not yet added)
        try:
          de_identified = export_row['de_identified'] or False
        except Exception:
          de_identified = False
        log_info(f"De-identified export: {de_identified}")

        # NEW: Read ai_optimized flag (defaults to False if column not yet added)
        try:
          ai_optimized = bool(export_row['ai_optimized'])
        except Exception:
          ai_optimized = False
        log_info(f"ai_optimized: {ai_optimized}")
        
        # Run the export
        log_info(f"Calling ai_export_generate for {team}")
        result = ai_export_generate(
          league=league,
          team=team,
          player_filter=player_filter,
          player_data_map=player_data_map,
          user=user,
          datasets_included=datasets_included,
          de_identified=de_identified,
          ai_optimized=ai_optimized 
        )

        # Update row with results
        if result['status'] == 'success':
          #export_row['disabled'] = True
          export_row['completed_at'] = datetime.now()
          export_row['files_generated'] = len(result['files'])
          export_row['result_message'] = result['message']
          export_row['file_list'] = json.dumps(result['files'])

          log_info(f"SUCCESS: Generated {len(result['files'])} files for {team}")
          email_text += f"✓ SUCCESS: Generated {len(result['files'])} files\n"

          # Track for force_index (uses the last successfully processed league/team)
          last_league = league
          last_team = team

        else:
          #export_row['disabled'] = True
          export_row['completed_at'] = datetime.now()
          export_row['result_message'] = result['message']

          log_error(f"ERROR: {result['message']}")
          email_text += f"✗ ERROR: {result['message']}\n"

        if user_email:
          send_export_notification(user_email, result, league, team)

      except Exception as e:
        log_exception('error', f"Error processing export row {idx}", e)
        email_text += f"✗ EXCEPTION: {str(e)}\n"
        export_row['disabled'] = True
        export_row['completed_at'] = datetime.now()
        export_row['result_message'] = str(e)

    # -------------------------------------------------------
    # Force index — only if we actually processed something
    # -------------------------------------------------------
    if last_league and last_team:
      log_info("All exports complete. Triggering force index for NotebookLM...")
      try:
        folder_path = ["Beach Internals Reports", last_league, last_team, "notebooklm"]
        force_index_player_files(folder_path)
        log_info("Force index complete.")
      except Exception as e:
        log_error(f"Force index failed: {str(e)}")
    else:
      log_info("No exports were processed — skipping force index.")

    # -------------------------------------------------------
    # Summary email
    # -------------------------------------------------------
    now1 = datetime.now()
    email_text += f"\n{'='*60}\n"
    email_text += f"AI Export Manager Completed at: {str(now1)}\n"
    email_text += f"Compute time: {str(now1-now)}\n"

    log_info(f"AI Export Manager - Completed. Compute time: {str(now1-now)}")

    internals_email = 'info@beachinternals.com'
    try:
      anvil.email.send(
        to=internals_email,
        from_address="no-reply",
        subject='Beach Internals - AI Export Manager Complete',
        text=email_text
      )
      log_info("Summary email sent to admin")
    except Exception as e:
      log_exception('error', "Error sending summary email", e)

    return True

  except Exception as e:
    log_exception('critical', "CRITICAL ERROR in ai_export_mgr_generate_background", e)
    raise





#--------------------------------------------------------------
# Direct callable function (for manual/immediate exports)
#--------------------------------------------------------------
#@anvil.server.callable
@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def ai_export_generate(league, team, 
                       date_start=None, 
                       date_end=None, 
                       player_filter=None, 
                       player_data_map=None, 
                       user=None, 
                       datasets_included=None, 
                       de_identified=False,
                       ai_optimized=False
                      ):
  """
    Generate NotebookLM-ready markdown files for AI analysis.
    
    Args:
        league: League identifier (e.g., 'NCAA')
        team: Team name (e.g., 'FSU')
        date_start: Optional start date for filtering data
        date_end: Optional end date for filtering data
        player_filter: Optional list of specific player names
        player_data_map: Dict mapping player_name to their full data (league, gender, year, etc.)
    
    Returns:
        Dictionary with status and list of generated files
    """
  #_require_own_team(team)
  _validate_league_params(league=league, team=team)

  try:
    log_info(f"Starting AI export for {team} in {league}")
    print(f"Starting AI export for {team} in {league}")

    # FIXED: Just use player_filter directly!
    if not player_filter:
      error_msg = 'No players specified for export (player_filter is required)'
      log_error(f"AI Export - {error_msg}")
      return {
        'status': 'error',
        'message': error_msg,
        'files': []
      }

    players = player_filter  # That's it!
    log_info(f"Generating exports for {len(players)} players: {players}")


    # Verify we have player data
    if not player_data_map:
      log_error("player_data_map is missing")
      return {'status': 'error', 'message': 'Missing player data map', 'files': []}

    generated_files = []

    # --- Build de-identification lookup ONCE before the player loop ---
    # We derive league/gender/year from the first player in player_data_map.
    # Uses print() so this always shows up even if log limit is hit later.
    deident_lookup = None
    if de_identified and player_data_map:
      try:
        first_player_data = next(iter(player_data_map.values()))
        di_league = first_player_data.get('league', league)
        di_gender = first_player_data.get('gender', '')
        di_year   = str(first_player_data.get('year', ''))
        print(f"DE-IDENT: building lookup for {di_league}/{di_gender}/{di_year}")
        if di_league and di_gender and di_year:
          deident_lookup = build_deident_lookup(di_league, di_gender, di_year)
          n_players = len(deident_lookup.get('player_map', {}))
          n_teams   = len(deident_lookup.get('team_map', {}))
          print(f"DE-IDENT: lookup built — {n_players} player keys, {n_teams} teams")
          log_info(f"De-identification lookup built: {n_players} player keys, {n_teams} teams")
        else:
          print(f"DE-IDENT ERROR: missing league/gender/year — di_league={di_league}, di_gender={di_gender}, di_year={di_year}")
          log_error("Cannot build de-ident lookup — missing league/gender/year in player_data_map")
      except Exception as e:
        print(f"DE-IDENT ERROR: {str(e)}")
        log_exception('error', "Error building de-identification lookup", e)

    # Generate one consolidated markdown file per player
    for player in players:
      try:
        log_info(f"Generating markdown for player: {player}")

        # Get player data if available
        player_data = player_data_map.get(player) if player_data_map else None

        file_info = generate_player_markdown(
          league=league,
          team=team,
          player=player,
          date_start=date_start,
          date_end=date_end,
          player_data=player_data,
          user=user,
          datasets_included=datasets_included,
          deident_lookup=deident_lookup,   
          de_identified=de_identified,
          ai_optimized=ai_optimized
        )
        if file_info:
          generated_files.append(file_info)
          log_info(f"Successfully generated file for {player}: {file_info['filename']}")
      except Exception as e:
        log_exception('error', f"Error generating file for {player}", e)
        print(f"Error generating file for {player}: {str(e)}")
        continue

    log_info(f"Successfully generated {len(generated_files)} markdown files")
    print(f"Successfully generated {len(generated_files)} markdown files")

    # --- NEW: If de-identified, write the player key file to the same Drive folder ---
    if de_identified and generated_files:
      try:
        # Build the key map from all successfully generated files
        key_map = []
        for f in generated_files:
          if f.get('player_uuid'):  # Only include files that were actually de-identified
            # Look up fullname from master_player using real player name
            real_name = f.get('player_real', f['player'])
            parts = real_name.split()
            fullname = real_name  # fallback
            if len(parts) >= 3:
              try:
                mp_rows = list(app_tables.master_player.search(
                  team=parts[0], number=int(parts[1]), shortname=' '.join(parts[2:])
                ))
                if mp_rows:
                  fullname = mp_rows[0]['fullname'] or real_name
              except Exception:
                pass
            key_map.append({
              'uuid': f['player_uuid'],
              'fullname': fullname,
              'shortname': parts[2] if len(parts) >= 3 else real_name,
              'team': parts[0] if parts else team,
              'number': parts[1] if len(parts) >= 2 else ''
            })

        if key_map:
          key_content = generate_player_key_file(key_map)
          # Save to same folder as the player files
          # Derive folder from the first file's path
          first_file_path = generated_files[0].get('path', '')
          if first_file_path:
            folder_parts = [p.strip() for p in first_file_path.split('/')]
          else:
            # Build default folder
            from generate_player_metrics_markdown import generate_player_metrics_markdown as _md
            folder_parts = ["Beach Internals Reports", league, team, "notebooklm-ai-export"]

          import anvil.media
          key_media = anvil.media.from_bytes(
            key_content.encode('utf-8'),
            content_type='text/markdown',
            name='00_Player_Key.md'
          )
          write_to_nested_folder(folder_parts, '00_Player_Key.md', key_media)
          log_info(f"Player key file written with {len(key_map)} entries")
      except Exception as e:
        log_exception('error', "Error writing player key file", e)

    return {
      'status': 'success',
      'message': f'Generated {len(generated_files)} player files',
      'files': generated_files
    }

  except Exception as e:
    log_exception('error', f"Error in ai_export_generate for {team}", e)
    print(f"Error in ai_export_generate: {str(e)}")
    return {
      'status': 'error',
      'message': str(e),
      'files': []
    }


#--------------------------------------------------------------
# Helper function to send individual export notifications
#--------------------------------------------------------------
def send_export_notification(user_email, result, league, team):
  """Send email notification for completed export."""
  try:
    if result['status'] == 'success':
      email_body = f"""
AI Export Complete for {team}

League: {league}
Team: {team}
Files Generated: {len(result['files'])}

Generated Files:
"""
      for file_info in result['files']:
        email_body += f"\n- {file_info['filename']}"
        email_body += f"\n  {file_info.get('result', 'Saved to Google Drive')}"
        email_body += f"\n  ({file_info['sessions_count']} sessions, {file_info['word_count']} words)\n"

      email_body += f"""

Files are saved in: Beach Internals Reports/{league}/{team}/notebooklm/

You can find these files in your Google Drive at the path above.
These files are ready to upload to NotebookLM for AI analysis.

Best regards,
Beach Internals AI Export Manager
"""
    else:
      email_body = f"""
AI Export Failed for {team}

League: {league}
Team: {team}
Error: {result['message']}

Please check the export settings and try again.

Best regards,
Beach Internals AI Export Manager
"""

    anvil.email.send(
      to=user_email,
      from_address="no-reply",
      subject=f'AI Export Complete - {team}',
      text=email_body
    )
  except Exception as e:
    print(f"Error sending notification email: {str(e)}")


#--------------------------------------------------------------
# Helper function to add export request to table
#--------------------------------------------------------------
@anvil.server.callable
@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def ai_export_add_request(team, player_filter=None, date_start=None, date_end=None, 
                          export_type='full', user_email=None):
  """
    Add a new export request to the ai_export_mgr table.
    League is automatically derived from the selected players.
    
    Args:
        team: Team name
        player_filter: List of player rows from master player table (or None for all players)
        date_start: Start date for data filter
        date_end: End date for data filter
        export_type: 'full', 'incremental', or 'custom'
        user_email: Email to notify when complete
    
    Returns:
        The created row
    """
  _require_own_team(team)
  _validate_league_params(team=team)
  try:
    # Derive league from player_filter if provided
    league = None
    if player_filter and len(player_filter) > 0:
      leagues_found = set()
      for player_row in player_filter:
        player_league = player_row.get('league')
        if player_league:
          leagues_found.add(player_league)

      if len(leagues_found) == 0:
        raise ValueError("No league found for selected players")
      elif len(leagues_found) > 1:
        raise ValueError(f"Players from multiple leagues selected: {leagues_found}. All players must be from the same league.")

      league = list(leagues_found)[0]

      # If no players specified, league must be provided separately or leave None
      # (will need to be filled in manually or code calling this needs to provide league)

    new_row = app_tables.ai_export_mgr.add_row(
      league=league,  # May be None if no players selected - will derive later
      team=team,
      date_start=date_start,
      date_end=date_end,
      export_type=export_type,
      player_filter=player_filter,  # Now accepts linked rows
      user_email=user_email,
      disabled=False,  # Start as enabled
      created_at=datetime.now(),
      files_generated=0
    )
    return new_row
  except Exception as e:
    print(f"Error adding export request: {str(e)}")
    return None


#--------------------------------------------------------------
# Helper function to add export request to table
#--------------------------------------------------------------
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def get_team_players(league, team):
  """
    Get list of players for a team from the 'master_player' table.
    Returns player names in format: "TEAM NUMBER SHORTNAME"
    """
  log_info(f"Getting players for {team} in {league}")
  try:
    # Query the master_player table
    players_query = app_tables.master_player.search(
      league=league,
      team=team
    )

    # Build player names in your format: "TEAM NUMBER SHORTNAME"
    players = []
    for row in players_query:
      try:
        player_name = f"{row['team']} {row['number']} {row['shortname']}"
        players.append(player_name)
      except (KeyError, AttributeError) as e:
        log_error(f"Error building player name: {e}")
        continue

        # Remove duplicates and sort
    players = sorted(list(set(players)))
    log_info(f"Found {len(players)} players for {team}")
    return players

  except AttributeError as e:
    if "No such app table" in str(e):
      log_error(f"Table 'master_player' not found: {e}")
      log_error("Please verify the table name matches your Anvil database")
    else:
      log_exception('error', f"Error getting players for {team}", e)
    return []
  except Exception as e:
    log_exception('error', f"Error getting players for {team}", e)
    return []


#--------------------------------------------------------------
# NEW: Helper to look up player_uuid from master_player table
#--------------------------------------------------------------
def get_player_uuid(team, number, shortname, league=None, gender=None, year=None):
  """
  Look up the player_uuid field from master_player table.
  Returns the uuid string, or None if not found.
  
  Args:
      team: Player's team (e.g. 'FSU')
      number: Player's jersey number (int)
      shortname: Player's short name (e.g. 'Johnson')
      league: Optional - narrows the search
      gender: Optional - narrows the search
      year: Optional - narrows the search
  """
  try:
    search_kwargs = dict(team=team, number=number, shortname=shortname)
    if league:
      search_kwargs['league'] = league
    if gender:
      search_kwargs['gender'] = gender
    if year:
      search_kwargs['year'] = str(year)  # master_player stores year as string

    rows = list(app_tables.master_player.search(**search_kwargs))
    if rows:
      uuid = rows[0]['player_uuid']
      if uuid:
        return str(uuid)
      log_error(f"player_uuid is empty for {team} {number} {shortname}")
    else:
      log_error(f"No master_player row found for {team} {number} {shortname}")
    return None
  except Exception as e:
    log_exception('error', f"Error looking up player_uuid for {team} {number} {shortname}", e)
    return None


#--------------------------------------------------------------
# NEW: Generate the player key file (UUID -> real identity mapping)
#--------------------------------------------------------------
def generate_player_key_file(player_key_map):
  """
  Build the 00_Player_Key.md file content mapping UUIDs back to real players.
  This file is saved in the same Drive folder as the de-identified exports.

  Args:
      player_key_map: list of dicts, each with keys:
          uuid, fullname, shortname, team, number

  Returns:
      str: Markdown content for the key file
  """
  lines = []
  lines.append("# Player Identity Key\n")
  lines.append("This file maps de-identified UUIDs back to real player identities.\n")
  lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n")
  lines.append("| UUID | Full Name | Short Name | Team | Number |\n")
  lines.append("| :--- | :--- | :--- | :--- | :--- |\n")
  for entry in player_key_map:
    lines.append(
      f"| {entry['uuid']} | {entry['fullname']} | {entry['shortname']} "
      f"| {entry['team']} | {entry['number']} |\n"
    )
  lines.append("\n---\n")
  lines.append("*Keep this file secure. Do not share with unauthorised parties.*\n")
  return ''.join(lines)


#--------------------------------------------------------------
# NEW: Build a full UUID lookup map for ALL players in a league/gender/year
# Called once per export run, then reused for every player's post-processing
#--------------------------------------------------------------
def build_deident_lookup(league, gender, year):
  """
  Query master_player for ALL players in this league/gender/year and build
  a dict mapping "TEAM NUMBER SHORTNAME" -> "Player_<uuid>".

  Also maps team names to "[Team_<uuid_prefix>]" so team names are redacted too.

  Args:
      league: e.g. 'NCAA'
      gender: e.g. 'W'
      year:   e.g. '2026' (string)

  Returns:
      dict with two keys:
        'player_map'  : { "FSU 10 Danielle": "Player_abc123", ... }
        'team_map'    : { "FSU": "[Team_abc123]", ... }
                         (only for shortnames that are unique in this dataset)
  """
  log_info(f"Building de-identification lookup for {league}/{gender}/{year}...")
  player_map   = {}
  team_uuids   = {}   # team -> first uuid seen (for team redaction)

  try:
    rows = list(app_tables.master_player.search(
      league=league,
      gender=gender,
      year=str(year)
    ))
    log_info(f"  Found {len(rows)} master_player rows for lookup")

    for row in rows:
      try:
        uuid = str(row['player_uuid']) if row['player_uuid'] else None
        if not uuid:
          continue
        t  = str(row['team'])
        n  = str(row['number'])
        sn = str(row['shortname'])
        display = f"Player_{uuid}"

        # Full "TEAM NUMBER SHORTNAME" pattern.
        # Store BOTH the raw number string from the table AND the plain
        # integer version, so we match regardless of zero-padding.
        # e.g. table has "05" -> keys "FSU 05 EJ" AND "FSU 5 EJ"
        # e.g. table has "5"  -> keys "FSU 5 EJ"  AND "FSU 05 EJ"
        player_map[f"{t} {n} {sn}"] = display
        try:
          n_int = int(n)
          # Add plain integer version (no zero-pad)
          plain = str(n_int)
          if plain != n:
            player_map[f"{t} {plain} {sn}"] = display
          # Add zero-padded version (2 digits)
          padded = f"{n_int:02d}"
          if padded != n and padded != plain:
            player_map[f"{t} {padded} {sn}"] = display
        except ValueError:
          pass  # number wasn't numeric, skip variants

        # Track team -> uuid prefix for team redaction
        if t not in team_uuids:
          team_uuids[t] = uuid[:6]

      except Exception as e:
        log_error(f"  Error processing master_player row: {e}")
        continue

    # Build team_map: team name -> redacted label
    team_map = {t: f"[Team_{uid}]" for t, uid in team_uuids.items()}

    log_info(f"  Lookup built: {len(player_map)} players, {len(team_map)} teams")

    return {
      'player_map': player_map,
      'team_map':   team_map
    }

  except Exception as e:
    log_exception('error', "Error building de-identification lookup", e)
    return {'player_map': {}, 'team_map': {}}


#--------------------------------------------------------------
# NEW: Post-process markdown content to replace all real names with UUIDs
#--------------------------------------------------------------
def deidentify_markdown(content, deident_lookup):
  """
  Replace ALL player names, partner names, opponent names, and team names
  in a markdown string using the pre-built lookup dict.

  Replacement order:
    1. Full "TEAM NUMBER SHORTNAME" patterns  (most specific — do first)
    2. Unique shortnames                       (catches headers and paragraphs)
    3. Team names                              (least specific — do last)

  Args:
      content: markdown string
      deident_lookup: dict from build_deident_lookup()

  Returns:
      str: redacted markdown content
  """
  if not content or not deident_lookup:
    return content

  player_map = deident_lookup.get('player_map', {})
  team_map   = deident_lookup.get('team_map', {})

  # 1. Replace full player names (e.g. "STETSON 10 Danielle")
  #    This covers: report headers, partner names, opponent names.
  #    Keys include both zero-padded and plain number variants.
  for real_name, display in player_map.items():
    content = content.replace(real_name, display)

  # NOTE: shortname-only replacement removed — it caused corruption by
  # matching shortnames inside already-replaced UUID display strings.
  # The full TEAM NUMBER SHORTNAME match above is sufficient.

  # 2. Replace team names (e.g. "STETSON" -> "[Team_abc123]")
  #    Use word-boundary-like approach: only replace when followed by space,
  #    newline, punctuation, or end of string to avoid partial matches.
  import re
  for team_name, display in team_map.items():
    # Replace team name when it appears as a standalone word
    content = re.sub(
      r'\b' + re.escape(team_name) + r'\b',
      display,
      content
    )

  return content


#--------------------------------------------------------------
# Core function to generate markdown for a single player
# FIXED: Now reads datasets_included from ai_export_mgr row
#--------------------------------------------------------------
@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def generate_player_markdown(league, team, player, date_start=None, date_end=None, 
                             player_data=None, user=None, datasets_included=None,
                             de_identified=False, deident_lookup=None, ai_optimized=False):
  """
  Generate a single combined markdown file for one player.
  Loops through each dataset row in datasets_included and appends
  each section into one combined file.
  
  Args:
      league: League identifier (e.g., 'NCAA')
      team: Team name (e.g., 'STETSON')
      player: Player name in format "TEAM NUMBER SHORTNAME"
      date_start: Optional start date filter
      date_end: Optional end date filter
      player_data: Dict with player's league, gender, year, shortname, etc.
      user: User context
      datasets_included: List of rows from ai_export_dataset_list table
      de_identified: If True, replace player name with player_uuid in filename
                     and markdown content. Real identity only in key file.
  
  Returns:
      dict with file info, or None on failure
  """
  log_info(f"Generating markdown for {player} ({team})")

  # --- Build league_value string ---
  if player_data:
    league_value   = player_data['league_value']
    player_shortname = player_data['shortname']
  else:
    # Fallback: parse player name
    parts = player.split()
    if len(parts) >= 3:
      player_shortname = ' '.join(parts[2:])
      league_value = f"{league} | W | 2026"   # best-guess fallback
      log_error(f"WARNING: Using fallback league_value={league_value} for {player}")
    else:
      log_error(f"Invalid player name format: {player}")
      return None

  log_info(f"Using player_data: league_value={league_value}, shortname={player_shortname}")

  # --- De-identification: resolve display name and UUID ---
  player_uuid = None
  if de_identified:
    if player_data:
      player_uuid = get_player_uuid(
        team=player_data['team'],
        number=player_data['number'],
        shortname=player_data['shortname'],
        league=player_data.get('league'),
        gender=player_data.get('gender'),
        year=player_data.get('year')
      )
    if player_uuid:
      display_name = f"Player_{player_uuid}"
      log_info(f"De-identified: {player} -> {display_name}")
    else:
      log_error(f"De-identification requested but no UUID found for {player} - using real name")
      display_name = player
  else:
    display_name = player

  # --- Base filters shared by all datasets ---
  base_filters = {
    'player': player,
    'player_shortname': player_shortname
  }
  if date_start:
    base_filters['start_date'] = date_start
  if date_end:
    base_filters['end_date'] = date_end

  # --- Decide which datasets to generate ---
  # If no datasets_included passed, fall back to the single aggregate (old behaviour)
  if not datasets_included:
    log_info("No datasets_included provided - falling back to single aggregate dataset")
    datasets_included = []   # will trigger fallback block below

  # --- Generate each dataset section and collect markdown strings ---
  combined_sections = []
  datasets_summary = []
  total_sets = 0
  total_words = 0

  if datasets_included:
    # Sort by the 'order' column so sections appear in the right sequence
    try:
      sorted_datasets = sorted(datasets_included, key=lambda r: r['order'] if r['order'] else 99)
    except Exception:
      sorted_datasets = list(datasets_included)

    for ds_row in sorted_datasets:
      ds_name        = ds_row['dataset_name']
      ds_type        = ds_row['dataset_type']      # 'aggregate' or 'set_level'
      function_name  = ds_row['function_name']     # e.g. 'generate_aggregate_section'
      ds_id          = ds_row['dataset_id']        # e.g. 'player_aggregate'
      section_title  = ds_row['output_section_title'] or ds_name
      comp_l1        = ds_row['comp_l1']           # e.g. 'Regular Season', 'Pre-Season', or ''
      days_before    = ds_row['days_before']       # e.g. 7, or None
      ds_date_start  = ds_row['date_start']        # dataset-level override, or ''
      ds_date_end    = ds_row['date_end']          # dataset-level override, or ''

      log_info(f"  Processing dataset: {ds_name} (type={ds_type}, fn={function_name})")

      try:
        # Build filters for this specific dataset
        ds_filters = base_filters.copy()

        # Apply comp_l1 filter if specified (e.g. 'Regular Season')
        if comp_l1:
          ds_filters['comp_l1'] = comp_l1

        # Apply days_before filter if specified (e.g. last 7 days)
        if days_before:
          #from datetime import datetime, timedelta
          cutoff = datetime.now() - timedelta(days=int(days_before))
          ds_filters['start_date'] = cutoff.strftime('%Y-%m-%d')
          ds_filters.pop('end_date', None)  # remove end_date so we go to today

        # Apply dataset-level date overrides if present
        if ds_date_start:
          ds_filters['start_date'] = ds_date_start
        if ds_date_end:
          ds_filters['end_date'] = ds_date_end

        # --- Call the right generation function ---
        if function_name == 'generate_player_metrics' or ds_type == 'aggregate':
          log_info(f"  Calling generate_player_metrics_markdown for {ds_name}...")
          result = generate_player_metrics_markdown(
            league_value=league_value,
            team=team,
            use_direct_data=True,
            ai_optimized=ai_optimized,
            **ds_filters
          )
          if result and 'media_obj' in result:
            section_md = result['media_obj'].get_bytes().decode('utf-8')
            sets_count = result['summary'].get('total_sets_analyzed', 0)
          else:
            log_error(f"  generate_player_metrics_markdown returned no result for {ds_name}")
            section_md = f"\n## {section_title}\n\n*No data available for this dataset.*\n"
            sets_count = 0

        elif function_name == 'generate_set_level_section' or ds_type == 'set_level':
          log_info(f"  Calling generate_set_level_metrics_for_player for {ds_name}...")

          # Fetch PPR data for this player with dataset-specific filters
          league_parts = league_value.split('|')
          league_str = league_parts[0].strip()
          gender_str = league_parts[1].strip()
          year_str   = league_parts[2].strip()

          ppr_df = get_filtered_ppr_data_direct(
            league=league_str,
            gender=gender_str,
            year=year_str,
              team=team,
              **ds_filters
          )

          if ppr_df is None or len(ppr_df) == 0:
              log_error(f"  No PPR data for set-level section {ds_name}")
              section_md = f"\n## {section_title}\n\n*No data available for this dataset.*\n"
              sets_count = 0
          else:
              log_info(f"  Loaded {len(ppr_df)} points for set-level calculation")
              set_level_data = generate_set_level_metrics_for_player(
                  ppr_df=ppr_df,
                  player_name=player,
                  league_value=league_value,
                  team=team
              )

              if set_level_data:
                  section_md = format_set_level_data_as_markdown(set_level_data,ai_optimized=ai_optimized)
                  sets_count = set_level_data.get('summary', {}).get('total_sets', 0)
              else:
                  log_error(f"  generate_set_level_metrics_for_player returned no result for {ds_name}")
                  section_md = f"\n## {section_title}\n\n*No data available for this dataset.*\n"
                  sets_count = 0
        else:
          log_error(f"  Unknown function_name '{function_name}' for dataset {ds_name} - skipping")
          section_md = f"\n## {section_title}\n\n*Dataset type not supported: {function_name}*\n"
          sets_count = 0

        # Wrap the section with a clear header
        section_header = f"\n\n---\n\n# DATASET: {section_title}\n\n"
        combined_sections.append(section_header + section_md)
        total_sets = max(total_sets, sets_count)   # use the max sets seen
        total_words += len(section_md.split())

        datasets_summary.append({
          'dataset': ds_name,
          'success': True,
          'sets': sets_count
        })
        log_info(f"  ✓ Dataset {ds_name}: {sets_count} sets, {len(section_md.split())} words")

      except Exception as e:
        log_exception('error', f"Error generating dataset {ds_name} for {player}", e)
        combined_sections.append(f"\n\n---\n\n# DATASET: {section_title}\n\n*Error generating this section: {str(e)}*\n")
        datasets_summary.append({'dataset': ds_name, 'success': False, 'error': str(e)})

  else:
    # ---- Fallback: no datasets_included, just run the single aggregate (old behaviour) ----
    log_info("Fallback: generating single aggregate markdown")
    log_info(f"Calling generate_player_metrics_markdown with league_value='{league_value}', team='{team}', filters={base_filters}")
    result = generate_player_metrics_markdown(
      league_value=league_value,
      team=team,
      use_direct_data=True,
      ai_optimized=ai_optimized,
      **base_filters
    )
    if not result or 'media_obj' not in result:
      log_error(f"Failed to generate metrics markdown for {player}")
      return None

    section_md = result['media_obj'].get_bytes().decode('utf-8')
    combined_sections.append(section_md)
    total_sets  = result['summary'].get('total_sets_analyzed', 0)
    total_words = len(section_md.split())
    datasets_summary.append({'dataset': 'Player Aggregate Statistics', 'success': True})

  # --- Combine all sections into one markdown string ---
  if not combined_sections:
    log_error(f"No sections generated for {player}")
    return None

  full_markdown = ''.join(combined_sections)

  # --- De-identification post-processing ---
  # Replace ALL real player/team names in the content using the lookup map.
  # This catches names that appear inside headers, partner lines, opponent
  # lines, and any other free text — regardless of which generation function
  # produced them.
  if de_identified and deident_lookup:
    n_player_keys = len(deident_lookup.get('player_map', {}))
    print(f"DE-IDENT POST-PROCESS: running on {len(full_markdown)} chars, {n_player_keys} player keys")
    log_info(f"Running de-identification post-processing on {len(full_markdown)} chars...")
    full_markdown = deidentify_markdown(full_markdown, deident_lookup)
    print(f"DE-IDENT POST-PROCESS: complete for {display_name}")
    log_info("De-identification post-processing complete")
  elif de_identified and not deident_lookup:
    print(f"DE-IDENT WARNING: de_identified=True but deident_lookup is None — {player} NOT redacted!")
    log_error("de_identified=True but no deident_lookup provided — content NOT fully redacted!")

  # --- Build filename ---
  # Use display_name: UUID-based if de_identified, real name otherwise
  player_safe = display_name.replace(' ', '_')
  parsed_league = league_value.replace(' ', '').replace('|', '_').strip('_')
  filename = f"{player_safe}_{parsed_league}_combined.md"
  log_info(f"Generated filename: {filename}")
  log_info(f"Combined markdown size: {len(full_markdown)} bytes, {total_words} words, {len(datasets_summary)} datasets")

  # --- Save to Google Drive ---
  log_info(f"Saving markdown file: {filename}")
  file_info = save_markdown_to_drive_updated(filename, full_markdown, league, team, player_data)

  if not file_info:
    log_error(f"Failed to save file for {player}")
    return None

  log_info(f"Successfully saved file for {player}: {file_info.get('url', 'No URL')}")

  return {
    'player': display_name,          # UUID-based name if de_identified
    'player_real': player,           # Always the real name (for key file)
    'player_uuid': player_uuid,      # None if not de_identified
    'filename': filename,
    'file_id': file_info.get('id', 'saved_to_drive'),
    'file_url': file_info.get('url', None),
    'path': file_info.get('path', 'unknown'),
    'datasets': [d['dataset'] for d in datasets_summary],
    'sessions_count': total_sets,
    'word_count': total_words,
    'summary': {
      'player': display_name,
      'league': league_value,
      'team': team,
      'datasets_included': datasets_summary,
      'generation_timestamp': str(datetime.now())
    }
  }



#--------------------------------------------------------------
# Get all session data for a player
#--------------------------------------------------------------
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def get_player_sessions(league, team, player, date_start=None, date_end=None, player_data=None, user=None):
  """
    Retrieve player metrics using the existing generate_player_metrics_json function.
    
    Args:
        league: League identifier
        team: Team name
        player: Player name in format "TEAM NUMBER SHORTNAME"
        date_start: Optional start date
        date_end: Optional end date
        player_data: Dict with player's league, gender, year, shortname, etc.
        user: User context for accessing data
    
    Returns list with single session containing all metrics.
    """
  log_info(f"Getting metrics for {player} ({team}, {league})")

  try:
    # Use player_data if provided, otherwise parse from player name
    if player_data:
      league_value = player_data['league_value']  # Already formatted: "NCAA | W | 2025"
      player_shortname = player_data['shortname']
      log_info(f"Using player_data: league_value={league_value}, shortname={player_shortname}")
    else:
      # Fallback: parse player name (shouldn't happen in normal use)
      log_error(f"No player_data provided for {player} - using fallback!")
      parts = player.split()
      if len(parts) >= 3:
        player_team = parts[0]
        player_number = parts[1]
        player_shortname = ' '.join(parts[2:])  # Handle multi-word names
        # Build league_value - this is a GUESS without player_data
        league_value = f"{league} | W | 2025"  # HARDCODED FALLBACK - NOT IDEAL!
        log_error(f"WARNING: Using hardcoded fallback league_value={league_value}. This may be incorrect!")
      else:
        log_error(f"Invalid player name format: {player}")
        return []

        # Build filters for generate_player_metrics_json
    json_filters = {
      'player': player,
      'player_shortname': player_shortname
    }

    # Add date filters if provided
    if date_start:
      json_filters['start_date'] = date_start
    if date_end:
      json_filters['end_date'] = date_end

    log_info(f"Calling generate_player_metrics_json with league_value='{league_value}', team='{team}', filters={json_filters}")

    # ISSUE: generate_player_metrics_json checks for logged-in user
    # SOLUTION: Call the underlying data functions directly using the team parameter

    # Import the data functions directly
    from generate_player_metrics_json_server import (
    get_filtered_ppr_data, 
    get_filtered_triangle_data,
    calculate_all_metrics
    )

    # Parse league_value to get components
    str_loc = league_value.index("|")
    league_part = league_value[:str_loc].strip()
    league_value_remainder = league_value[str_loc + 1:]
    str_loc = league_value_remainder.index("|")
    gender = league_value_remainder[:str_loc].strip()
    year = str(int(league_value_remainder[str_loc + 1:].strip()))

    log_info(f"Parsed league_value: league={league_part}, gender={gender}, year={year}")
        
        # Get PPR data directly (bypassing user check)
    log_info("Retrieving PPR data directly...")
    ppr_df = get_filtered_ppr_data_direct(league_part, gender, year, team, **json_filters)
        
    if len(ppr_df) == 0:
            log_error("No PPR data found")
            return []
        
    log_info(f"Retrieved {len(ppr_df)} PPR data points")
        
        # Get triangle data
    log_info("Retrieving triangle data...")
    tri_df = get_filtered_triangle_data(league_part, gender, year, team, **json_filters)
    log_info(f"Retrieved {len(tri_df)} triangle data sets")
        
        # Load metric dictionary
    log_info("Loading metric dictionary...")
    dict_rows = list(app_tables.metric_dictionary.search())
    column_names = [col['name'] for col in app_tables.metric_dictionary.list_columns()]
    metric_dict = pd.DataFrame([{col: row[col] for col in column_names} for row in dict_rows])
    log_info(f"Loaded {len(metric_dict)} metrics")
        
        # Calculate metrics
    log_info("Calculating metrics...")
    metrics_result = calculate_all_metrics(metric_dict, ppr_df, tri_df, player)
    log_info(f"Calculated {metrics_result['successful']} metrics")
        
        # Build metadata
    metadata = {
            'generated_at': datetime.now().isoformat(),
            'player_name': player,
            'player_shortname': player_shortname,
            'league': league_part,
            'gender': gender,
            'year': year,
            'team': team,
            'total_points_analyzed': len(ppr_df),
            'total_sets_analyzed': len(tri_df),
        }
        
        # Build metrics dict organized by category
    metrics_by_category = metrics_result['metrics']
        
    log_info(f"Got {len(metrics_by_category)} metric categories for {player}")
        
        # Return as a single "session" with all metrics
    session = {
            'session_id': 'full_season',
            'date': metadata['generated_at'],
            'opponent': 'All Opponents',
            'partner': 'All Partners',
            'session_type': 'Season Analysis',
            'result': 'N/A',
            'total_points_analyzed': metadata['total_points_analyzed'],
            'total_sets_analyzed': metadata['total_sets_analyzed'],
            'metrics': metrics_by_category  # All 492 metrics organized by category
        }
        
    return [session]  # Return as list with one session
        
  except Exception as e:
        log_exception('error', f"Error getting metrics for {player}", e)
        return []


#--------------------------------------------------------------
# Extract metrics from a single match/session
#--------------------------------------------------------------
@monitor_performance(level=MONITORING_LEVEL_VERBOSE)
def extract_session_metrics(match_row, player):
    """
    Extract all available metrics from a match row.
    
    Returns dictionary with all metrics for this session.
    """
    
    # This is where you'll extract your 500+ metrics per session
    # Adjust field names to match your actual data structure
    
    session = {
        'session_id': match_row.get('match_id', 'unknown'),
        'date': str(match_row.get('date', '')),
        'opponent': match_row.get('opponent', 'Unknown'),
        'partner': match_row.get('partner', 'Unknown'),
        'session_type': match_row.get('match_type', 'Match'),
        'result': match_row.get('result', 'Unknown'),
        
        # Performance metrics (add all your 500+ metrics here)
        'metrics': {}
    }
    
    # Extract all numeric metrics
    # This is a placeholder - you'll expand this based on your data
    metric_fields = [
        'total_attacks', 'attack_kills', 'attack_errors', 'attack_pct',
        'serve_aces', 'serve_errors', 'serve_total',
        'block_kills', 'block_assists', 'block_errors',
        'dig_total', 'reception_total', 'reception_errors',
        'set_assists', 'set_errors',
        # Add all your other metrics here...
    ]
    
    for field in metric_fields:
        if field in match_row:
            session['metrics'][field] = match_row[field]
    
    return session


#--------------------------------------------------------------
# Build the markdown content following Gemini's format
#--------------------------------------------------------------
@monitor_performance(level=MONITORING_LEVEL_DETAILED)
def build_markdown_content(player, team, league, sessions_data, date_start=None, date_end=None):
    """
    Build markdown content following the NotebookLM format:
    1. Metadata header
    2. Coach's Quick Summary
    3. JSON data blocks
    """
    
    # Calculate summary statistics
    total_sessions = len(sessions_data)
    date_range = f"{date_start} to {date_end}" if date_start and date_end else "All available data"
    
    # Calculate some quick stats for the summary
    total_kills = sum(s['metrics'].get('attack_kills', 0) for s in sessions_data)
    total_attacks = sum(s['metrics'].get('total_attacks', 0) for s in sessions_data)
    avg_attack_pct = (total_kills / total_attacks * 100) if total_attacks > 0 else 0
    
    # Build markdown
    markdown_lines = [
        "# Player Performance Data Export",
        "",
        "## Metadata",
        f"- **Player Name:** {player}",
        f"- **Team:** {team}",
        f"- **League:** {league}",
        f"- **Date Range:** {date_range}",
        f"- **Total Sessions:** {total_sessions}",
        f"- **Export Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "---",
        "",
        "## Coach's Quick Summary",
        "",
        f"This file contains performance data for **{player}** from {team}.",
        f"The dataset includes {total_sessions} sessions with 500+ metrics per session.",
        "",
        f"**Key Performance Indicators:**",
        f"- Total Attack Kills: {total_kills}",
        f"- Total Attack Attempts: {total_attacks}",
        f"- Average Attack %: {avg_attack_pct:.1f}%",
        "",
        "The detailed metrics for each session are provided in JSON format below.",
        "This allows AI analysis tools to quickly parse and analyze patterns across sessions.",
        "",
        "---",
        "",
        "## Session Data (JSON Format)",
        "",
    ]
    
    # Add each session as a JSON block
    for i, session in enumerate(sessions_data, 1):
        markdown_lines.extend([
            f"### Session {i}: {session['date']} vs {session['opponent']}",
            "",
            "```json",
            json.dumps(session, indent=2),
            "```",
            ""
        ])
    
    # Add consolidated summary at the end
    markdown_lines.extend([
        "---",
        "",
        "## Consolidated Metrics Summary",
        "",
        "```json",
        json.dumps({
            'player': player,
            'team': team,
            'league': league,
            'total_sessions': total_sessions,
            'date_range': date_range,
            'all_sessions': sessions_data
        }, indent=2),
        "```"
    ])
    
    return "\n".join(markdown_lines)


#--------------------------------------------------------------
# Save markdown file to Google Drive
#--------------------------------------------------------------
@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def save_markdown_to_drive_updated(filename, content, league, team, player_data=None):
  """Save combined markdown to ai_player_files table - one row per player+team"""
  try:
    from datetime import timezone

    # Extract player_uuid from filename
    # Format: Player_PLYR-2878eccb_MARK_W_2026_combined.md
    parts = filename.replace('_combined.md', '').split('_')
    player_uuid = parts[1] if len(parts) > 1 else None

    if not player_uuid:
      log_error(f"Could not extract player_uuid from filename: {filename}")
      return None

    # Create media object
    media = anvil.BlobMedia('text/markdown', content.encode('utf-8'))

    # One row per player_uuid + team — always overwrite
    existing = app_tables.ai_player_files.get(
      player_uuid=player_uuid,
      team=team
    )
    if existing:
      existing['combined_file'] = media
      existing['updated_at'] = datetime.now(timezone.utc)
      log_info(f"Updated ai_player_files: {player_uuid} / {team}")
    else:
      app_tables.ai_player_files.add_row(
        player_uuid=player_uuid,
        team=team,
        combined_file=media,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc)
      )
      log_info(f"Created ai_player_files: {player_uuid} / {team}")

    return {
      'id': player_uuid,
      'url': None,
      'path': 'ai_player_files table',
      'result': 'saved to table'
    }

  except Exception as e:
    log_error(f"Error saving to ai_player_files: {str(e)}")
    return None
    





def get_or_create_nested_folder(folder_path):
  """
    Navigate to (or create) nested folders using proper Anvil API.
    
    Args:
        folder_path: List of folder names ['NCAAW2026', 'STETSON', 'notebooklm']
        
    Returns:
        Anvil folder object (or None on failure)
    """
  try:
    current_folder = None

    for folder_name in folder_path:
      log_debug(f"Looking for folder: {folder_name}")

      # List files in current location
      if current_folder:
        # List files in this folder
        files_list = current_folder.list()
      else:
        # List files in root
        files_list = app_files.list()

        # Search for matching folder
      found_folder = None
      for item in files_list:
        try:
          item_name = item.get_name()
          if item_name == folder_name:
            # Check if it's a folder (has list-like behavior)
            try:
              list(item)  # Folders are iterable
              found_folder = item
              log_debug(f"Found existing folder: {folder_name}")
              break
            except:
              # Not a folder, skip
              continue
        except:
          continue

      if found_folder:
        current_folder = found_folder
      else:
        # Create folder
        log_info(f"Creating folder: {folder_name}")
        if current_folder:
          current_folder = current_folder.create_folder(folder_name)
        else:
          current_folder = app_files.create_folder(folder_name)
        log_info(f"Created folder: {folder_name}")

    return current_folder

  except Exception as e:
    log_error(f"Error in get_or_create_nested_folder: {str(e)}")
    import traceback
    log_error(traceback.format_exc())
    return None


def search_file_in_folder(filename, folder_obj):
  """
    Search for a file by name within a folder object.
    
    Args:
        filename: Name of file to search for
        folder_obj: Anvil folder object
        
    Returns:
        Anvil file object if found, None otherwise
    """
  try:
    log_debug(f"Searching for file: {filename}")

    # List files in the folder
    files_list = folder_obj.list()

    for item in files_list:
      try:
        item_name = item.get_name()
        if item_name == filename:
          log_info(f"Found existing file: {filename}")
          return item
      except:
        continue

    log_debug(f"File not found: {filename}")
    return None

  except Exception as e:
    log_error(f"Error searching for file: {str(e)}")
    return None


def update_drive_file(file_obj, media_body):
  """
    Update an existing file's content.
    
    Args:
        file_obj: Anvil file object
        media_body: BlobMedia with new content
        
    Returns:
        Updated file object
    """
  try:
    log_debug(f"Updating file content...")

    # Update the file content
    file_obj.set_bytes(media_body.get_bytes())

    log_debug("File content updated successfully")
    return file_obj

  except Exception as e:
    log_error(f"Error updating file: {str(e)}")
    import traceback
    log_error(traceback.format_exc())
    return None


def create_drive_file(filename, media_body, parent_folder):
  """
    Create a new file in a folder.
    
    Args:
        filename: Name of the file
        media_body: BlobMedia with content
        parent_folder: Anvil folder object
        
    Returns:
        Created file object
    """
  try:
    log_debug(f"Creating new file: {filename}")

    # Create file in the folder
    new_file = parent_folder.create(media_body, name=filename)

    log_debug("File created successfully")
    return new_file

  except Exception as e:
    log_error(f"Error creating file: {str(e)}")
    import traceback
    log_error(traceback.format_exc())
    return None



#--------------------------------------------------------------
# Utility function to check file size and consolidation needs
#--------------------------------------------------------------
def check_consolidation_needs(sessions_data):
    """
    Check if data needs to be split across multiple files.
    NotebookLM limit: 500k words per file.
    
    Returns list of session groups if splitting is needed.
    """
    
    # Estimate word count
    total_json = json.dumps(sessions_data)
    word_count = len(total_json.split())
    
    MAX_WORDS = 450000  # Leave buffer below 500k
    
    if word_count < MAX_WORDS:
        return [sessions_data]  # One file is fine
    
    # Need to split - divide sessions into groups
    words_per_session = word_count / len(sessions_data)
    sessions_per_file = int(MAX_WORDS / words_per_session)
    
    groups = []
    for i in range(0, len(sessions_data), sessions_per_file):
        groups.append(sessions_data[i:i + sessions_per_file])
    
    return groups


def process_export_job_markdown(export_row):
  """
    Process export job - MARKDOWN VERSION.
    
    This is the NEW version that generates Markdown instead of JSON.
    Add this function to your existing ai_export_manager module.
    
    Args:
        export_row: Row from ai_export_mgr table
        
    Returns:
        dict: {
            'files_generated': int,
            'file_list': list of dicts with file info,
            'message': str
        }
    """

  log_info(f"Processing MARKDOWN export for {export_row['team']} ({export_row['league']})")

  ai_optimized = bool(export_row['ai_optimized'])
  log_info(f"ai_optimized: {ai_optimized}")

  # Extract export parameters
  league = export_row['league']
  team = export_row['team']
  date_start = export_row['date_start']
  date_end = export_row['date_end']
  export_type = export_row['export_type']
  player_filter = export_row['player_filter']  # Linked rows from master_player

  # Determine year and gender
  # ADJUST THIS based on how your data stores league/gender/year
  year = 2026  # Or extract from somewhere

  if 'W' in league or 'WOMEN' in league.upper():
    gender = 'W'
  elif 'M' in league or 'MEN' in league.upper():
    gender = 'M'
  else:
    gender = 'W'  # Default

  log_info(f"Parsed: League={league}, Gender={gender}, Year={year}, Team={team}")

  # Build filters
  filters = {'team': team}
  if date_start:
    filters['start_date'] = date_start
  if date_end:
    filters['end_date'] = date_end

    # Get player list
  if not player_filter:
    log_error("No players specified in player_filter")
    return {'files_generated': 0, 'file_list': [], 'message': 'No players specified'}

  player_list = list(player_filter)
  log_info(f"Found {len(player_list)} players to export")

  # Set up output folder
  league_gender_year = f"{league}{gender}{year}"
  output_folder = [league_gender_year, team, 'PlayerAI']
  log_info(f"Output folder: {' / '.join(output_folder)}")

  # Generate global context file once
  # not doing this, we make this file manually and update it there
  '''
  try:
    log_info("Generating global context file...")
    context_file = generate_global_context_markdown()
    context_result = write_to_nested_folder(
      output_folder,
      '00_Global_Context_Philosophy.md',
      context_file
    )
    log_info(f"Global context file: {context_result}")
  except Exception as e:
    log_error(f"Failed to create global context: {e}")
  '''

    # Generate Markdown file for each player
  file_list = []
  files_generated = 0

  for player_idx, player_row in enumerate(player_list):
    log_info(f"\n--- Player {player_idx + 1}/{len(player_list)} ---")

    try:
      # Extract player info
      player_fullname = player_row['fullname']
      player_shortname = player_row['shortname']
      player_number = str(player_row['number'])
      player_team = player_row['team']

      # Build player display name
      player_name = f"{player_team} {player_number} {player_shortname}"
      log_info(f"Processing: {player_name}")

      # Build league_value format
      league_value = f"{league} | {gender} | {year}"

      # Add player-specific filters
      player_filters = filters.copy()
      player_filters['player'] = player_name
      player_filters['player_shortname'] = player_shortname

      # Generate Markdown export
      # Call the generation function (JSON or Markdown)
      if export_type in ['markdown', 'full']:
        log_info(f"Calling generate_player_metrics_markdown...")
        result = generate_player_metrics_markdown(
          league_value=league_value,
          team=team,
          ai_optimized=ai_optimized,
          **player_filters
        )
      else:
        log_info(f"Calling generate_player_metrics_json...")
        result = generate_player_metrics_json(
          league_value=league_value,
          team=team,
          **player_filters
        )

      if result and result['media_obj']:
        # Get player_uuid from master_player
        master_row = app_tables.master_player.get(
          league=league,
          gender=gender,
          year=year,
          team=player_team,
          number=int(player_number)
        )

        if master_row and master_row['player_uuid']:
          player_uuid = master_row['player_uuid']
        else:
          # Fallback - generate one and save it
          import uuid
          player_uuid = f"PLYR-{str(uuid.uuid4())[:8]}"
          if master_row:
            master_row['player_uuid'] = player_uuid
          log_info(f"Generated new uuid for {player_name}: {player_uuid}")

        # Save to ai_player_files table
        from datetime import timezone
        existing = app_tables.ai_player_files.get(player_uuid=player_uuid)
        if existing:
          existing['combined_file'] = result['media_obj']
          existing['updated_at'] = datetime.now(timezone.utc)
          log_info(f"Updated ai_player_files: {player_uuid}")
        else:
          app_tables.ai_player_files.add_row(
            player_uuid=player_uuid,
            combined_file=result['media_obj'],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
          )
          log_info(f"Created ai_player_files: {player_uuid}")

        # Keep file_info for result tracking
        file_info = {
          'player': player_name,
          'filename': f"{player_uuid}_combined.md",
          'file_id': player_uuid,
          'file_url': None,
          'path': 'ai_player_files table',
          'sessions_count': result['summary']['total_sets_analyzed'],
          'word_count': len(result['media_obj'].get_bytes().decode('utf-8').split())
        }

        file_list.append(file_info)
        files_generated += 1
        log_info(f"✓ Saved: {player_name} as {player_uuid}")

      else:
        log_error(f"Failed to generate Markdown for {player_name}")

    except Exception as e:
      log_error(f"Error processing player {player_name}: {str(e)}", with_traceback=True)
      continue

    # Build result message
  message = f"Generated {files_generated} player files"
  if files_generated < len(player_list):
    message += f" ({len(player_list) - files_generated} failed)"

  return {
    'files_generated': files_generated,
    'file_list': file_list,
    'message': message
  }


def force_index_player_files(folder_path):
  """
    Loop through files in a folder and 'touch' them to force 
    Google Drive to index them for NotebookLM.
    """
  current_folder = app_files.reports
  for subfolder_name in folder_path:
    current_folder = current_folder.get(subfolder_name)
    if not current_folder:
      return "Folder path not found."

  files_touched = 0
  # Iterate through all files in the folder
  for file in current_folder.list_files():
    # 1. Update the description (This is a metadata 'touch')
    # We add a timestamp so Google sees a 'change'
    #import datetime
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    file.description = f"Indexed for NotebookLM at {timestamp}"

    # 2. Force a read (This forces the server to pull the bytes)
    _ = file.get_bytes()

    # 3. Re-affirm the title
    original_title = file.title
    file.title = original_title

    files_touched += 1
    print(f"Touched: {original_title}")

  return f"Successfully touched {files_touched} files. Try NotebookLM again in 2-3 minutes."

"""
Updated process_export_job function for ai_export_manager.py

This replaces the existing process_export_job_markdown function.
It now supports multiple datasets that can be combined into single or separate files.

ADD THIS TO ai_export_manager.py
"""

@monitor_performance(level=MONITORING_LEVEL_CRITICAL)
def process_export_job_with_datasets(export_row):
  """
    Process export job with support for multiple datasets.
    
    This is the NEW version that supports:
    - Multiple datasets per export (player aggregate, set-level, etc.)
    - Both combined and separate file output
    - Both markdown and JSON formats
    
    Args:
        export_row: Row from ai_export_mgr table
        
    Returns:
        dict: {
            'files_generated': int,
            'file_list': list of dicts with file info,
            'message': str
        }
    """
  log_info(f"Processing multi-dataset export for {export_row['team']} ({export_row['league']})")

  # Extract export parameters
  league = export_row['league']
  team = export_row['team']
  date_start = export_row['date_start']
  date_end = export_row['date_end']
  export_type = export_row['export_type'] or 'markdown'
  player_filter = export_row['player_filter']  # Linked rows from master_player

  # NEW: Get datasets to include
  from ai_export_dataset_combiner import get_enabled_datasets
  datasets_to_include = get_enabled_datasets(export_row)

  if not datasets_to_include:
    log_error("No datasets enabled for this export")
    return {'files_generated': 0, 'file_list': [], 'message': 'No datasets enabled'}

  log_info(f"Datasets to include: {[ds['dataset_name'] for ds in datasets_to_include]}")
  
  # Get player list
  if not player_filter:
    log_error("No players specified in player_filter")
    return {'files_generated': 0, 'file_list': [], 'message': 'No players specified'}

  player_list = list(player_filter)
  log_info(f"Found {len(player_list)} players to export")
  
  # Extract year and gender from player data (like the rest of the code does)
  # Get player list
  if not player_filter:
    log_error("No players specified in player_filter")
    return {'status': 'error', 'message': 'No players specified', 'files': []}

  player_list = list(player_filter)
  log_info(f"Found {len(player_list)} players to export")

  # Get year and gender from first player (all players should have same league/gender/year)
  first_player = player_list[0]
  year = str(first_player['year'])  # Convert to string for table query
  gender = first_player['gender']

  log_info(f"Extracted from player data: League={league}, Gender={gender}, Year={year}, Team={team}")

  # Build filters
  filters = {}
  if date_start:
    filters['start_date'] = date_start
  if date_end:
    filters['end_date'] = date_end

  # Set up output folder
  league_gender_year = f"{league}{gender}{year}"
  output_folder = [league_gender_year, team, 'notebooklm-ai-export']
  log_info(f"Output folder: {' / '.join(output_folder)}")

  # Generate global context file once
  '''
  try:
    log_info("Generating global context file...")
    from generate_player_metrics_markdown import generate_global_context_markdown
    context_file = generate_global_context_markdown()
    context_result = write_to_nested_folder(
      output_folder,
      '00_Global_Context_Philosophy.md',
      context_file
    )
    log_info(f"Global context file created")
  except Exception as e:
    log_error(f"Failed to create global context: {str(e)}")
  '''
  
    # Determine output format
  if export_type in ['markdown', 'full']:
    output_format = 'markdown'
  elif export_type == 'json':
    output_format = 'json'
  else:
    output_format = 'markdown'  # Default

  log_info(f"Output format: {output_format}")

  # Generate files for each player
  file_list = []
  files_generated = 0

  for player_idx, player_row in enumerate(player_list):
    log_info(f"\n--- Player {player_idx + 1}/{len(player_list)} ---")

    try:
      # Extract player info
      player_fullname = player_row['fullname']
      player_shortname = player_row['shortname']
      player_number = str(player_row['number'])
      player_team = player_row['team']

      # Build player display name
      player_name = f"{player_team} {player_number} {player_shortname}"
      log_info(f"Processing: {player_name}")

      # Build league_value format
      league_value = f"{league} | {gender} | {year}"

      # Add player-specific filters
      player_filters = filters.copy()
      player_filters['player'] = player_name
      player_filters['player_shortname'] = player_shortname

      # Get PPR data for this player
      log_info("Fetching PPR data...")
      ppr_df = get_filtered_ppr_data_direct(
        league=league,
        gender=gender,
        year=year,
        team=team,
        **player_filters
      )

      if ppr_df is None or len(ppr_df) == 0:
        log_error(f"No PPR data found for {player_name}")
        continue

      log_info(f"Loaded {len(ppr_df)} points for {player_name}")

      # Generate combined export with all datasets
      from ai_export_dataset_combiner import generate_combined_player_export

      result = generate_combined_player_export(
        ppr_df=ppr_df,
        player_name=player_name,
        league_value=league_value,
        team=team,
        datasets_to_include=datasets_to_include,
        output_format=output_format,
        ai_optimized=ai_optimized  
      )

      if result['success'] and result['media_obj']:
        # Upload to Google Drive
        log_info(f"Uploading to Drive: {result['filename']}")
        upload_result = write_to_nested_folder(
          output_folder,
          result['filename'],
          result['media_obj']
        )

        # Count words/size
        content_bytes = result['media_obj'].get_bytes()
        word_count = len(content_bytes.decode('utf-8').split())

        # Extract sessions_count from the new summary structure
        # The summary has datasets_included array, each with their own summary
        sessions_count = 0
        for dataset_info in result['summary'].get('datasets_included', []):
          if 'summary' in dataset_info and 'total_sets' in dataset_info['summary']:
            sessions_count = max(sessions_count, dataset_info['summary']['total_sets'])

        # Track file info
        file_info = {
          'player': player_name,
          'filename': result['filename'],
          'file_id': 'saved_to_drive',
          'file_url': None,
          'path': ' / '.join(output_folder),
          'datasets': [ds['dataset_name'] for ds in datasets_to_include],
          'sessions_count': sessions_count,  # Now properly extracted
          'word_count': word_count,
          'summary': result['summary']
        }

        file_list.append(file_info)
        files_generated += 1

        log_info(f"✓ Generated: {result['filename']} ({word_count} words)")

      else:
        log_error(f"Failed to generate export for {player_name}")

    except Exception as e:
      log_error(f"Error processing player {player_name}: {str(e)}")
      import traceback
      log_error(traceback.format_exc())
      continue

    # Build result message
  message = f"Generated {files_generated} player files with {len(datasets_to_include)} dataset(s) each"
  if files_generated < len(player_list):
    message += f" ({len(player_list) - files_generated} failed)"

  # Return in same format as ai_export_generate for consistency
  return {
    'status': 'success' if files_generated > 0 else 'error',
    'message': message,
    'files': file_list  # Changed from 'file_list' to 'files'
  }