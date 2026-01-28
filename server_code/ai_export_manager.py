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

# Import pandas for data processing
import pandas as pd
import io

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

  try:
    # Get all rows from ai_export_mgr table where NOT disabled
    # This allows re-running exports by just unchecking 'disabled'
    export_rows = app_tables.ai_export_mgr.search(
      tables.order_by('created_at', ascending=True)
    )

    # Filter out disabled rows
    # Note: Use bracket notation, not .get() - Anvil rows don't support .get()
    # Handle None values - treat None as False (not disabled)
    export_rows = [row for row in export_rows if row['disabled'] != True]

    total_rows = len(export_rows)
    email_text += f"Found {total_rows} enabled export requests\n\n"
    log_info(f"AI Export Manager - Processing {total_rows} export requests")
    print(f"Processing {total_rows} export requests")

    # Process each row
    for idx, export_row in enumerate(export_rows, 1):
      try:
        log_info(f"AI Export Manager - Processing export {idx} of {total_rows}")
        email_text += f"\n{'='*60}\n"
        email_text += f"Processing export {idx} of {total_rows}\n"
        email_text += f"{'='*60}\n"

        # Extract parameters from the row
        team = export_row['team']
        date_start = export_row['date_start']
        date_end = export_row['date_end']
        export_type = export_row['export_type'] or 'full'
        user_email = export_row['user_email']

        log_info(f"Export row data: team={team}, export_type={export_type}, date_start={date_start}, date_end={date_end}")

        # NEW: Handle player_filter as linked rows from master player table
        player_filter_rows = export_row['player_filter']
        player_filter = None
        league = export_row['league']  # May be None - we'll derive it

        log_info(f"Player filter rows: {player_filter_rows}, type: {type(player_filter_rows)}")
        if player_filter_rows:
          log_info(f"Number of players selected: {len(player_filter_rows) if player_filter_rows else 0}")

        if player_filter_rows and len(player_filter_rows) > 0:
          # Convert linked rows to list of player names and derive league
          player_filter = []
          leagues_found = set()
          player_data_map = {}  # Map player_name to their full data

          for player_row in player_filter_rows:
            # Get player name in YOUR format: "TEAM NUMBER SHORTNAME"
            # Example: "FSU 12 Johnson"
            try:
              team_val = player_row['team']
              number_val = player_row['number']
              shortname_val = player_row['shortname']
              player_name = f"{team_val} {number_val} {shortname_val}"

              # Get league components
              league_val = player_row['league']
              gender_val = player_row['gender']
              year_val = player_row['year']

              # Build league_value string for generate_player_metrics_json
              league_value = f"{league_val} | {gender_val} | {year_val}"

              log_info(f"Built player: {player_name}, league_value: {league_value}")

              # Store player data
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

              # Collect league from each player for validation
            if league_val:
              leagues_found.add(league_val)

              # Validate: all players must be from same league
          if len(leagues_found) == 0:
            raise ValueError("No league found for selected players")
          elif len(leagues_found) > 1:
            raise ValueError(f"Players from multiple leagues selected: {leagues_found}. All players must be from the same league.")

            # Use the league from the players (overrides table league if present)
          league = list(leagues_found)[0]
          log_info(f"League derived from players: {league}")
          email_text += f"League (derived from players): {league}\n"
        else:
          # No player filter - must have league specified in table
          if not league:
            raise ValueError("Either league or player_filter must be specified")
          log_info(f"League from table: {league}")
          email_text += f"League (from table): {league}\n"
          player_data_map = {}  # Empty map when no player filter

        email_text += f"Team: {team}\n"
        email_text += f"Date Range: {date_start} to {date_end}\n"
        email_text += f"Export Type: {export_type}\n"
        if player_filter:
          email_text += f"Players: {', '.join(player_filter)}\n"
          log_info(f"Players: {', '.join(player_filter)}")
        else:
          email_text += f"Players: All players on team\n"
          log_info("Players: All players on team")

          # Mark as processing (set started_at timestamp)
        export_row['started_at'] = datetime.now()

        # Store the derived league back to the row
        export_row['league'] = league

        # Generate the exports
        log_info(f"Calling ai_export_generate for {team}")
        result = ai_export_generate(
          league=league,
          team=team,
          date_start=date_start,
          date_end=date_end,
          player_filter=player_filter,
          player_data_map=player_data_map,  # Pass the full player data
          user=user  # Pass user context
        )

        # Update row with results
        if result['status'] == 'success':
          export_row['disabled'] = True  # Disable the row after successful export
          export_row['completed_at'] = datetime.now()
          export_row['files_generated'] = len(result['files'])
          export_row['result_message'] = result['message']

          log_info(f"SUCCESS: Generated {len(result['files'])} files for {team}")
          email_text += f"✓ SUCCESS: Generated {len(result['files'])} files\n"

          # Store file info in JSON
          export_row['file_list'] = json.dumps(result['files'])

        else:
          export_row['disabled'] = True  # Also disable on error (prevents retry loop)
          export_row['completed_at'] = datetime.now()
          export_row['result_message'] = result['message']

          log_error(f"ERROR: {result['message']}")
          email_text += f"✗ ERROR: {result['message']}\n"

          # Send individual notification if email provided
        if user_email:
          send_export_notification(user_email, result, league, team)

      except Exception as e:
        log_exception('error', f"Error processing export row {idx}", e)
        email_text += f"✗ EXCEPTION: {str(e)}\n"
        export_row['disabled'] = True  # Disable on exception
        export_row['completed_at'] = datetime.now()
        export_row['result_message'] = str(e)

        # Send summary email to admin
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
      print(f"Error sending summary email: {str(e)}")

    return True

  except Exception as e:
    log_exception('critical', "CRITICAL ERROR in ai_export_mgr_generate_background", e)
    raise


#--------------------------------------------------------------
# Direct callable function (for manual/immediate exports)
#--------------------------------------------------------------
@anvil.server.callable
@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def ai_export_generate(league, team, date_start=None, date_end=None, player_filter=None, player_data_map=None, user=None):
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
  try:
    log_info(f"Starting AI export for {team} in {league}")
    print(f"Starting AI export for {team} in {league}")

    # Get player list for the team
    players = get_team_players(league, team)

    # Apply player filter if provided
    if player_filter:
      players = [p for p in players if p in player_filter]
      log_info(f"Filtered to {len(players)} players: {players}")

    if not players:
      error_msg = 'No players found for this team'
      log_error(f"AI Export - {error_msg}: {team} in {league}")
      return {
        'status': 'error',
        'message': error_msg,
        'files': []
      }

    log_info(f"Generating exports for {len(players)} players")
    generated_files = []

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
          player_data=player_data,  # Pass individual player's data
          user=user  # Pass user context
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
# Core function to generate markdown for a single player
#--------------------------------------------------------------
@monitor_performance(level=MONITORING_LEVEL_IMPORTANT)
def generate_player_markdown(league, team, player, date_start=None, date_end=None, player_data=None, user=None):
  """
    Generate a single consolidated markdown file for one player.
    
    Args:
        league: League identifier
        team: Team name
        player: Player name in format "TEAM NUMBER SHORTNAME"
        date_start: Optional start date
        date_end: Optional end date
        player_data: Dict with player's league, gender, year, etc.
    
    Returns file info dict with path and metadata.
    """
  log_info(f"Generating markdown for {player} ({team})")

  # 1. Gather all session/match data for this player
  sessions_data = get_player_sessions(league, team, player, date_start, date_end, player_data, user)

  if not sessions_data:
    log_error(f"No data found for {player}")
    print(f"No data found for {player}")
    return None

  log_info(f"Found {len(sessions_data)} sessions for {player}")

  # 2. Build the markdown content
  markdown_content = build_markdown_content(
    player=player,
    team=team,
    league=league,
    sessions_data=sessions_data,
    date_start=date_start,
    date_end=date_end
  )

  # 3. Create filename (safe for filesystems)
  safe_player_name = player.replace(' ', '_').replace('/', '_')
  filename = f"{safe_player_name}_{league}_{team}.md"

  # 4. Save to Google Drive
  log_info(f"Saving markdown file: {filename}")
  file_info = save_markdown_to_drive(filename, markdown_content, league, team, player_data)

  if not file_info:
    log_error(f"Failed to save file for {player}")
    print(f"Failed to save file for {player}")
    return None

  log_info(f"Successfully saved file for {player}: {file_info.get('url', 'No URL')}")

  return {
    'player': player,
    'filename': filename,
    'file_id': file_info.get('id', 'unknown'),
    'file_url': file_info.get('url', None),
    'path': file_info.get('path', 'unknown'),
    'sessions_count': len(sessions_data),
    'word_count': len(markdown_content.split())
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
def save_markdown_to_drive(filename, content, league, team, player_data=None):
    """
    Save markdown file to Google Drive using write_to_nested_folder.
    
    Folder structure: reports/[league][gender][year]/[team]/notebooklm/
    Example: reports/NCAAW2026/STETSON/notebooklm/
    
    Returns the file path/ID and URL.
    """
    
    try:
        from server_functions import write_to_nested_folder
        
        # Build league string in format: NCAAW2026 (league + gender + year)
        if player_data:
            league_str = f"{player_data['league']}{player_data['gender']}{player_data['year']}"
        else:
            # Fallback if no player_data - just use league as-is
            league_str = league
        
        # Folder path: ['reports', 'NCAAW2026', 'STETSON', 'notebooklm']
        # take the 'reports' out of he league string
        folder_path = [ league_str, team, 'notebooklm']
        
        log_info(f"Saving to folder: {' / '.join(folder_path)}")
        
        # Create the file media (markdown content as BlobMedia)
        media = anvil.BlobMedia('text/markdown', content.encode('utf-8'), name=filename)
        
        # Use write_to_nested_folder (same as report manager)
        result = write_to_nested_folder(folder_path, filename, media)
        
        log_info(f"write_to_nested_folder returned: {result}")
        
        return {
            'id': 'saved_to_drive',
            'url': None,
            'path': ' / '.join(folder_path),
            'result': str(result)
        }
        
    except Exception as e:
        log_exception('error', f"Error saving markdown to Google Drive", e)
        print(f"Error saving to Google Drive: {str(e)}")
        
        # Fallback: save to Anvil's data files table if it exists
        try:
            row = app_tables.ai_export_files.add_row(
                filename=filename,
                content=content,
                league=league,
                team=team,
                created=datetime.now()
            )
            
            log_info(f"Fallback: Saved to ai_export_files table (row ID: {row.get_id()})")
            
            return {
                'id': row.get_id(),
                'url': None,
                'path': 'ai_export_files table'
            }
            
        except Exception as e2:
            log_exception('error', f"Fallback storage also failed", e2)
            print(f"Fallback storage failed: {str(e2)}")
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
  output_folder = [league_gender_year, team, 'notebooklm']
  log_info(f"Output folder: {' / '.join(output_folder)}")

  # Generate global context file once
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

      # Build player display name
      player_name = f"{team} {player_number} {player_shortname}"
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
        # Upload to Google Drive
        log_info(f"Uploading to Drive: {result['filename']}")
        upload_result = write_to_nested_folder(
          output_folder,
          result['filename'],
          result['media_obj']
        )

        # Count words
        md_content = result['media_obj'].get_bytes().decode('utf-8')
        word_count = len(md_content.split())

        # Track file info (matches your existing format from CSV)
        file_info = {
          'player': player_name,
          'filename': result['filename'],
          'file_id': 'saved_to_drive',
          'file_url': None,
          'path': ' / '.join(output_folder),
          'sessions_count': result['summary']['total_sets_analyzed'],
          'word_count': word_count
        }

        file_list.append(file_info)
        files_generated += 1

        log_info(f"✓ Generated: {result['filename']} ({word_count} words)")

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
