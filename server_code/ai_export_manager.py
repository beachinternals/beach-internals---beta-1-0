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

# Import your logging and performance monitoring utilities
from server_functions import logger, log_performance

# This is a server module for generating NotebookLM-ready markdown files
# with player performance data in JSON format
# Uses ai_export_mgr table for control (similar to rpt_mgr table for reports)

#--------------------------------------------------------------
# Helper function for logging errors with tracebacks
#--------------------------------------------------------------
def log_exception(logger, level, message, exception):
  """Helper to log exceptions with traceback using f-strings"""
  tb = traceback.format_exc()
  if level.lower() == 'critical':
    logger.critical(f"{message}: {str(exception)}\nTraceback:\n{tb}")
  elif level.lower() == 'error':
    logger.error(f"{message}: {str(exception)}\nTraceback:\n{tb}")
  elif level.lower() == 'warning':
    logger.warning(f"{message}: {str(exception)}\nTraceback:\n{tb}")
  elif level.lower() == 'info':
    logger.info(f"{message}: {str(exception)}\nTraceback:\n{tb}")

#--------------------------------------------------------------
# Main function - processes all rows in ai_export_mgr table
#--------------------------------------------------------------
@anvil.server.callable
@log_performance
def ai_export_mgr_generate():
  """
    Process all pending exports in the ai_export_mgr table.
    Similar to rpt_mgr_generate() in your report manager.
    
    Returns:
        True when complete
    """
  logger.info("AI Export Manager - Generate Called")
  print("Starting AI Export Manager...")
  now = datetime.now()
  email_text = f"AI Export Manager Started at: {str(now)}\n\n"

  # Launch as background task
  task = anvil.server.launch_background_task('ai_export_mgr_generate_background')
  logger.info("AI Export Manager - Background task launched")

  return True


@anvil.server.background_task
@log_performance
def ai_export_mgr_generate_background():
  """
    Background task that processes the ai_export_mgr table.
    """
  logger.info("AI Export Manager Background - Started")
  now = datetime.now()
  email_text = f"AI Export Manager Started at: {str(now)}\n\n"

  try:
    # Get all rows from ai_export_mgr table where status is 'pending'
    export_rows = app_tables.ai_export_mgr.search(
      status='pending'
    )

    total_rows = len(export_rows)
    email_text += f"Found {total_rows} pending export requests\n\n"
    logger.info(f"AI Export Manager - Processing {total_rows} export requests")
    print(f"Processing {total_rows} export requests")

    # Process each row
    for idx, export_row in enumerate(export_rows, 1):
      try:
        logger.info(f"AI Export Manager - Processing export {idx} of {total_rows}")
        email_text += f"\n{'='*60}\n"
        email_text += f"Processing export {idx} of {total_rows}\n"
        email_text += f"{'='*60}\n"

        # Extract parameters from the row
        team = export_row['team']
        date_start = export_row['date_start']
        date_end = export_row['date_end']
        export_type = export_row.get('export_type', 'full')
        user_email = export_row.get('user_email', None)

        # NEW: Handle player_filter as linked rows from master player table
        player_filter_rows = export_row.get('player_filter', None)
        player_filter = None
        league = export_row.get('league', None)  # May be None - we'll derive it

        if player_filter_rows and len(player_filter_rows) > 0:
          # Convert linked rows to list of player names and derive league
          player_filter = []
          leagues_found = set()

          for player_row in player_filter_rows:
            # Get player name (adjust field name to match your master player table)
            player_name = player_row.get('player_name') or player_row.get('name') or player_row.get('shortname')
            if player_name:
              player_filter.append(player_name)

              # Collect league from each player
            player_league = player_row.get('league')
            if player_league:
              leagues_found.add(player_league)

              # Validate: all players must be from same league
          if len(leagues_found) == 0:
            raise ValueError("No league found for selected players")
          elif len(leagues_found) > 1:
            raise ValueError(f"Players from multiple leagues selected: {leagues_found}. All players must be from the same league.")

            # Use the league from the players (overrides table league if present)
          league = list(leagues_found)[0]
          logger.info(f"League derived from players: {league}")
          email_text += f"League (derived from players): {league}\n"
        else:
          # No player filter - must have league specified in table
          if not league:
            raise ValueError("Either league or player_filter must be specified")
          logger.info(f"League from table: {league}")
          email_text += f"League (from table): {league}\n"

        email_text += f"Team: {team}\n"
        email_text += f"Date Range: {date_start} to {date_end}\n"
        email_text += f"Export Type: {export_type}\n"
        if player_filter:
          email_text += f"Players: {', '.join(player_filter)}\n"
          logger.info(f"Players: {', '.join(player_filter)}")
        else:
          email_text += f"Players: All players on team\n"
          logger.info("Players: All players on team")

          # Update status to 'processing'
        export_row['status'] = 'processing'
        export_row['started_at'] = datetime.now()

        # Store the derived league back to the row
        export_row['league'] = league

        # Generate the exports
        logger.info(f"Calling ai_export_generate for {team}")
        result = ai_export_generate(
          league=league,
          team=team,
          date_start=date_start,
          date_end=date_end,
          player_filter=player_filter
        )

        # Update row with results
        if result['status'] == 'success':
          export_row['status'] = 'complete'
          export_row['completed_at'] = datetime.now()
          export_row['files_generated'] = len(result['files'])
          export_row['result_message'] = result['message']

          logger.info(f"SUCCESS: Generated {len(result['files'])} files for {team}")
          email_text += f"✓ SUCCESS: Generated {len(result['files'])} files\n"

          # Store file info in JSON
          export_row['file_list'] = json.dumps(result['files'])

        else:
          export_row['status'] = 'error'
          export_row['completed_at'] = datetime.now()
          export_row['result_message'] = result['message']

          logger.error(f"ERROR: {result['message']}")
          email_text += f"✗ ERROR: {result['message']}\n"

          # Send individual notification if email provided
        if user_email:
          send_export_notification(user_email, result, league, team)

      except Exception as e:
        log_exception(logger, 'error', f"Error processing export row {idx}", e)
        email_text += f"✗ EXCEPTION: {str(e)}\n"
        export_row['status'] = 'error'
        export_row['completed_at'] = datetime.now()
        export_row['result_message'] = str(e)

        # Send summary email to admin
    now1 = datetime.now()
    email_text += f"\n{'='*60}\n"
    email_text += f"AI Export Manager Completed at: {str(now1)}\n"
    email_text += f"Compute time: {str(now1-now)}\n"

    logger.info(f"AI Export Manager - Completed. Compute time: {str(now1-now)}")

    internals_email = 'info@beachinternals.com'
    try:
      anvil.email.send(
        to=internals_email,
        from_address="no-reply",
        subject='Beach Internals - AI Export Manager Complete',
        text=email_text
      )
      logger.info("Summary email sent to admin")
    except Exception as e:
      log_exception(logger, 'error', "Error sending summary email", e)
      print(f"Error sending summary email: {str(e)}")

    return True

  except Exception as e:
    log_exception(logger, 'critical', "CRITICAL ERROR in ai_export_mgr_generate_background", e)
    raise


#--------------------------------------------------------------
# Direct callable function (for manual/immediate exports)
#--------------------------------------------------------------
@anvil.server.callable
@log_performance
def ai_export_generate(league, team, date_start=None, date_end=None, player_filter=None):
  """
    Generate NotebookLM-ready markdown files for AI analysis.
    
    Args:
        league: League identifier (e.g., 'NCAA_W_2025')
        team: Team name (e.g., 'FSU')
        date_start: Optional start date for filtering data
        date_end: Optional end date for filtering data
        player_filter: Optional list of specific player names
    
    Returns:
        Dictionary with status and list of generated files
    """
  try:
    logger.info(f"Starting AI export for {team} in {league}")
    print(f"Starting AI export for {team} in {league}")

    # Get player list for the team
    players = get_team_players(league, team)

    # Apply player filter if provided
    if player_filter:
      players = [p for p in players if p in player_filter]
      logger.info(f"Filtered to {len(players)} players: {players}")

    if not players:
      error_msg = 'No players found for this team'
      logger.warning(f"AI Export - {error_msg}: {team} in {league}")
      return {
        'status': 'error',
        'message': error_msg,
        'files': []
      }

    logger.info(f"Generating exports for {len(players)} players")
    generated_files = []

    # Generate one consolidated markdown file per player
    for player in players:
      try:
        logger.info(f"Generating markdown for player: {player}")
        file_info = generate_player_markdown(
          league=league,
          team=team,
          player=player,
          date_start=date_start,
          date_end=date_end
        )
        if file_info:
          generated_files.append(file_info)
          logger.info(f"Successfully generated file for {player}: {file_info['filename']}")
      except Exception as e:
        log_exception(logger, 'error', f"Error generating file for {player}", e)
        print(f"Error generating file for {player}: {str(e)}")
        continue

    logger.info(f"Successfully generated {len(generated_files)} markdown files")
    print(f"Successfully generated {len(generated_files)} markdown files")

    return {
      'status': 'success',
      'message': f'Generated {len(generated_files)} player files',
      'files': generated_files
    }

  except Exception as e:
    log_exception(logger, 'error', f"Error in ai_export_generate for {team}", e)
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

Generated Files (click to open in Google Drive):
"""
      for file_info in result['files']:
        if file_info.get('file_url'):
          email_body += f"\n- {file_info['filename']}"
          email_body += f"\n  {file_info['file_url']}"
          email_body += f"\n  ({file_info['sessions_count']} sessions, {file_info['word_count']} words)\n"
        else:
          email_body += f"\n- {file_info['filename']} ({file_info['sessions_count']} sessions)\n"

      email_body += f"""

Files are saved in: Beach Internals Reports/{league}/{team}/notebooklm/

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
      status='pending',
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
def get_team_players(league, team):
  """
    Get list of players for a team.
    You'll need to adjust this to match your data structure.
    """
  logger.info(f"Getting players for {team} in {league}")
  # This is a placeholder - adjust based on your actual data tables
  try:
    # Example: Query your player table
    players_query = app_tables.players.search(
      league=league,
      team=team
    )

    # Return list of unique player names
    players = list(set([row['player_name'] for row in players_query]))
    logger.info(f"Found {len(players)} players for {team}")
    return sorted(players)

  except Exception as e:
    log_exception(logger, 'warning', f"Error getting players from players table for {team}", e)
    # Fallback: try to get from match data
    try:
      matches = app_tables.matches.search(
        league=league,
        team=team
      )
      players = set()
      for match in matches:
        if hasattr(match, 'player1'):
          players.add(match['player1'])
        if hasattr(match, 'player2'):
          players.add(match['player2'])
      logger.info(f"Found {len(players)} players from matches table for {team}")
      return sorted(list(players))
    except Exception as e2:
      log_exception(logger, 'error', f"Error getting players from matches table for {team}", e2)
      return []


#--------------------------------------------------------------
# Core function to generate markdown for a single player
#--------------------------------------------------------------
def generate_player_markdown(league, team, player, date_start=None, date_end=None):
  """
    Generate a single consolidated markdown file for one player.
    
    Returns file info dict with path and metadata.
    """
  logger.info(f"Generating markdown for {player} ({team})")

  # 1. Gather all session/match data for this player
  sessions_data = get_player_sessions(league, team, player, date_start, date_end)

  if not sessions_data:
    logger.warning(f"No data found for {player}")
    print(f"No data found for {player}")
    return None

  logger.info(f"Found {len(sessions_data)} sessions for {player}")

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
  logger.info(f"Saving markdown file: {filename}")
  file_info = save_markdown_to_drive(filename, markdown_content, league, team)

  if not file_info:
    logger.error(f"Failed to save file for {player}")
    print(f"Failed to save file for {player}")
    return None

  logger.info(f"Successfully saved file for {player}: {file_info['file_url']}")

  return {
    'player': player,
    'filename': filename,
    'file_id': file_info['id'],
    'file_url': file_info['url'],
    'path': file_info['path'],
    'sessions_count': len(sessions_data),
    'word_count': len(markdown_content.split())
  }


#--------------------------------------------------------------
# Get all session data for a player
#--------------------------------------------------------------
def get_player_sessions(league, team, player, date_start=None, date_end=None):
  """
    Retrieve all match/session data for a player.
    
    Returns list of dictionaries, each containing session metrics.
    """
  logger.info(f"Getting sessions for {player} ({team}, {league})")
  sessions = []

  try:
    # Build query filters
    query_filters = {
      'league': league,
      'team': team
    }

    # Add date filters if provided
    if date_start:
      query_filters['date'] = q.greater_than_or_equal_to(date_start)
      logger.info(f"Filtering sessions after {date_start}")
    if date_end:
      query_filters['date'] = q.less_than_or_equal_to(date_end)
      logger.info(f"Filtering sessions before {date_end}")

      # Query your matches table (adjust to your schema)
    matches = app_tables.matches.search(**query_filters)

    match_count = 0
    for match in matches:
      # Check if player participated
      if match.get('player1') == player or match.get('player2') == player:
        match_count += 1
        # Extract all metrics for this session
        session_data = extract_session_metrics(match, player)
        sessions.append(session_data)

    logger.info(f"Found {len(sessions)} sessions for {player} (out of {match_count} matches)")
    return sessions

  except Exception as e:
    log_exception(logger, 'error', f"Error getting sessions for {player}", e)
    return []


#--------------------------------------------------------------
# Extract metrics from a single match/session
#--------------------------------------------------------------
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
def save_markdown_to_drive(filename, content, league, team):
  """
    Save markdown file to Google Drive in the same folder structure as reports.
    
    Folder structure: Beach Internals Reports/[league]/[team]/notebooklm/
    (Similar to reports but with 'notebooklm' instead of date folder)
    
    Returns the file path/ID and URL.
    """

  try:
    # Build folder path matching your report manager pattern
    # Instead of: ['Beach Internals Reports', league, team, '2025-01-26']
    # We use:     ['Beach Internals Reports', league, team, 'notebooklm']
    folder_path = ['Beach Internals Reports', league, team, 'notebooklm']

    print(f"Saving to folder: {' / '.join(folder_path)}")

    # Create the file media
    media = anvil.BlobMedia('text/markdown', content.encode('utf-8'), name=filename)

    # Upload to Google Drive using the folder path
    # This uses the same pattern as your report manager
    file = anvil.google.drive.app_files.create_in_folder(
      media,
      folder_path,
      title=filename
    )

    # Get the file URL for the return value
    file_url = f"https://drive.google.com/file/d/{file['id']}/view"

    print(f"Saved file: {filename} (ID: {file['id']})")

    return {
      'id': file['id'],
      'url': file_url,
      'path': ' / '.join(folder_path)
    }

  except Exception as e:
    print(f"Error saving to Google Drive: {str(e)}")
    # Fallback: save to Anvil's data files table
    try:
      from anvil import media as anvil_media

      # Create table if it doesn't exist (you may need to create this manually)
      row = app_tables.ai_export_files.add_row(
        filename=filename,
        content=anvil_media.from_file(content.encode('utf-8')),
        league=league,
        team=team,
        created=datetime.now()
      )

      return {
        'id': str(row.get_id()),
        'url': None,
        'path': 'Anvil Data Tables'
      }
    except Exception as e2:
      print(f"Fallback also failed: {str(e2)}")
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