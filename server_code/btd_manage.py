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
import numpy as np
import datetime

# This is a server module for BTD file management
# It provides functions to search, validate, and update BTD files

# ############ Get filtered list of BTD files for the management screen
@anvil.server.callable
def get_btd_files_filtered(filters=None):
  """
    Get filtered list of BTD files for the management grid
    
    filters = {
        'league': 'NCAA',
        'gender': 'W', 
        'year': 2025,
        'team': 'BEACHINTERNALS',
        'date_from': date object or None,
        'date_to': date object or None,
        'status': 'Private' or 'Scouting' or 'All',
        'min_completeness': 0-100 or None,
        'has_errors': True/False/None,
        'sort_by': 'date', 'completeness', 'per_xy', 'errors'
        'sort_desc': True/False
    }
    """

  # Start with base query
  query_args = {}

  if filters:
    # Add league/gender/year filters
    if filters.get('league'):
      query_args['league'] = filters['league']
    if filters.get('gender'):
      query_args['gender'] = filters['gender']
    if filters.get('year'):
      query_args['year'] = filters['year']
    if filters.get('team'):
      query_args['team'] = filters['team']

    # Execute the base query
  rows = app_tables.btd_files.search(**query_args)

  # Convert to list of dicts for easier filtering and sorting
  file_list = []
  for row in rows:
    # Additional filters that need to be applied after query

    # Date range filter
    if filters and filters.get('date_from'):
      if row['date'] < filters['date_from']:
        continue
    if filters and filters.get('date_to'):
      if row['date'] > filters['date_to']:
        continue

        # Status filter (Private/Scouting)
    if filters and filters.get('status') and filters['status'] != 'All':
      if filters['status'] == 'Private' and not row['private']:
        continue
      if filters['status'] == 'Scouting' and row['private']:
        continue

        # Completeness filter
    if filters and filters.get('min_completeness'):
      if row['completeness_score'] < filters['min_completeness']:
        continue

        # Has errors filter
    if filters and filters.get('has_errors'):
      if not row['no_errors'] or row['no_errors'] == 0:
        continue

        # Build the dict for this file
    file_dict = {
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
      'include_in_master': row['include_in_master'],
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
      'owner': row['owner'],
      'btd_file_date': row['btd_file_date']
    }

    file_list.append(file_dict)

    # Sort the results
  if filters and filters.get('sort_by'):
    sort_by = filters['sort_by']
    sort_desc = filters.get('sort_desc', True)

    if sort_by == 'date':
      file_list.sort(key=lambda x: x['date'] or datetime.date(1900, 1, 1), reverse=sort_desc)
    elif sort_by == 'completeness':
      file_list.sort(key=lambda x: x['completeness_score'] or 0, reverse=sort_desc)
    elif sort_by == 'per_xy':
      file_list.sort(key=lambda x: x['per_xy'] or 0, reverse=sort_desc)
    elif sort_by == 'errors':
      file_list.sort(key=lambda x: x['no_errors'], reverse=sort_desc)
  else:
    # Default sort by date, newest first
    file_list.sort(key=lambda x: x['date'] or datetime.date(1900, 1, 1), reverse=True)

  return file_list


# ############ Get complete details of a single BTD file
@anvil.server.callable
def get_btd_file_details(btd_file_id):
  """
    Load complete details of selected BTD file for editing
    Returns all fields as dict
    """

  row = app_tables.btd_files.get_by_id(btd_file_id)

  if not row:
    return None

    # Return complete record as dict
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
    'include_in_master': row['include_in_master'],
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
    'owner': row['owner'],
    'btd_file_date': row['btd_file_date'],
    'csv_data': row['csv_data']  # Include the actual file
  }


# ############ Enhanced CSV analysis with strict validation
@anvil.server.callable
def analyze_btd_csv_strict(csv_file):
  """
    Enhanced version of update_btd_characteristics
    Returns comprehensive validation results
    Auto-fixes NOPLAYER entries but reports all issues
    
    Returns: {
        'valid': True/False,
        'players': [list of exactly 4 player names] or [],
        'player_count_raw': actual count before cleaning,
        'invalid_players': [list of NOPLAYER, null, etc],
        'metrics': {
            'points': int,
            'completeness_score': float,
            'per_players': float,
            'per_xy': float,
            'per_srv_players': float
        },
        'errors': [list of error strings],
        'warnings': [list of warning strings]
    }
    """
    
  errors = []
  warnings = []
  invalid_players = []
    
  try:
        # Parse the CSV file
        file_obj = io.BytesIO(csv_file.get_bytes())
        btd_df = pd.read_csv(file_obj)
        
        # Handle new format with 'team' field (from balltime)
        if 'team' in btd_df.columns:
            btd_df = btd_df.fillna({'team': str('NOTEAM'), 'player': str('NOPLAYER')})
            btd_df = btd_df.rename(columns={'player': 'only_player'})
            btd_df['player'] = btd_df['team'].astype(str) + ' ' + btd_df['only_player'].astype(str)
        
        # Calculate number of actions
        num_actions = int(btd_df.shape[0])
        
        if num_actions == 0:
            errors.append("CSV file is empty or contains no actions")
            return {
                'valid': False,
                'players': [],
                'player_count_raw': 0,
                'invalid_players': [],
                'metrics': None,
                'errors': errors,
                'warnings': warnings
            }
        
        # Calculate serves (points)
        serves_df = btd_df[btd_df['action_type'] == "serve"]
        num_serves = int(serves_df.shape[0])
        
        if num_serves == 0:
            errors.append("No serves found in file - may not be a valid match file")
        
        # Calculate actions with player
        players_df = btd_df[btd_df['player'].notna()]
        per_action_players = int(players_df.shape[0]) / num_actions if num_actions > 0 else 0
        
        # Calculate serves with players
        srv_players = serves_df[serves_df['player'].notna()]
        per_srv_players = int(srv_players.shape[0]) / num_serves if num_serves > 0 else 0
        
        # Calculate actions with coordinates
        src_coord = btd_df[btd_df['src_zone'].notna()]
        dest_coord = btd_df[btd_df['dest_zone'].notna()]
        num_src_coord = int(src_coord.shape[0]) + int(dest_coord.shape[0])
        per_coord = num_src_coord / (2 * num_actions) if num_actions > 0 else 0
        
        # Create completeness score
        comp_score = (5 * per_srv_players + 3 * per_action_players + 2 * per_coord) / 10
        
        # Find unique players
        players_unique = players_df.player.unique()
        players_unique.sort()
        player_count_raw = players_unique.shape[0]
        
        # Check for invalid players and clean them
        tmp_players = players_unique.copy()
        num_deleted = 0
        
        for p in range(len(players_unique)):
            player_name = str(players_unique[p])
            
            # Check for NOPLAYER entries
            if "NOPLAYER" in player_name:
                invalid_players.append(player_name)
                tmp_players = np.delete(tmp_players, p - num_deleted)
                num_deleted += 1
                warnings.append(f"Removed invalid player: {player_name}")
        
        players_unique = tmp_players
        player_count_clean = len(players_unique)
        
        # Validate we have exactly 4 players after cleaning
        if player_count_clean != 4:
            errors.append(f"Expected exactly 4 players, found {player_count_clean} valid players (raw count: {player_count_raw})")
            
            if player_count_clean > 4:
                errors.append(f"Too many players: {', '.join(players_unique)}")
            elif player_count_clean < 4:
                errors.append(f"Too few players: {', '.join(players_unique)}")
                # Pad with Unknown to prevent crashes
                while len(players_unique) < 4:
                    players_unique = np.append(players_unique, "Unknown")
                    warnings.append("Added 'Unknown' placeholder for missing player")
        
        # Check for empty/null player names
        for player in players_unique[:4]:
            if not player or player.strip() == '' or player.lower() == 'nan':
                errors.append(f"Player name is empty or null: '{player}'")
                invalid_players.append(player)
        
        # Build result
        result = {
            'valid': len(errors) == 0,
            'players': list(players_unique[:4]),
            'player_count_raw': player_count_raw,
            'invalid_players': invalid_players,
            'metrics': {
                'points': num_serves,
                'completeness_score': round(comp_score * 100, 1),  # As percentage
                'per_players': round(per_action_players * 100, 1),
                'per_xy': round(per_coord * 100, 1),
                'per_srv_players': round(per_srv_players * 100, 1)
            },
            'errors': errors,
            'warnings': warnings
        }
        
        return result
        
  except Exception as e:
        errors.append(f"Failed to parse CSV file: {str(e)}")
        return {
            'valid': False,
            'players': [],
            'player_count_raw': 0,
            'invalid_players': [],
            'metrics': None,
            'errors': errors,
            'warnings': warnings
        }


# ############ Validate player mappings against master_player
@anvil.server.callable
def validate_player_mappings(league, gender, year, player_names):
    """
    Check if player names can be mapped to master_player table
    
    league: 'NCAA'
    gender: 'W'
    year: 2025
    player_names: [playera1, playera2, playerb1, playerb2] (from BTD file)
    
    Returns: {
        'valid': True/False,
        'mappings': [
            {
                'btd_name': 'TEAM1 J.Smith', 
                'found': True, 
                'matches': ['TEAM1 10 Smith', 'TEAM1 12 Smithson'], 
                'exact_match': 'TEAM1 10 Smith' or None,
                'suggested': 'TEAM1 10 Smith'
            },
            ...
        ]
    }
    """
    
    # Get all players for this league/gender/year
    master_players = app_tables.master_player.search(
        league=league,
        gender=gender,
        year=int(year)
    )
    
    # Build lookup list
    master_list = []
    for row in master_players:
        player_str = f"{row['team']} {row['number']} {row['shortname']}"
        master_list.append({
            'display': player_str,
            'team': row['team'],
            'number': row['number'],
            'shortname': row['shortname'],
            'fullname': row['fullname'],
            'alias1': row['alias1']
        })
    
    # Check each BTD player name
    mappings = []
    all_valid = True
    
    for btd_name in player_names:
        mapping = {
            'btd_name': btd_name,
            'found': False,
            'matches': [],
            'exact_match': None,
            'suggested': None
        }
        
        # Try to find matches
        for master_player in master_list:
            # Check if BTD name contains parts of master player name
            btd_lower = str(btd_name).lower()
            team_lower = str(master_player['team']).lower()
            short_lower = str(master_player['shortname']).lower()
            
            # Exact match on display string
            if master_player['display'].lower() == btd_lower:
                mapping['exact_match'] = master_player['display']
                mapping['found'] = True
                mapping['matches'].append(master_player['display'])
                mapping['suggested'] = master_player['display']
                break
            
            # Partial match - team and shortname in BTD name
            if team_lower in btd_lower and short_lower in btd_lower:
                mapping['found'] = True
                mapping['matches'].append(master_player['display'])
                if not mapping['suggested']:
                    mapping['suggested'] = master_player['display']
            
            # Check alias if present
            if master_player['alias1']:
                alias_lower = str(master_player['alias1']).lower()
                if alias_lower in btd_lower:
                    mapping['found'] = True
                    mapping['matches'].append(master_player['display'])
                    if not mapping['suggested']:
                        mapping['suggested'] = master_player['display']
        
        if not mapping['found']:
            all_valid = False
        
        mappings.append(mapping)
    
    return {
        'valid': all_valid,
        'mappings': mappings
    }


# ############ Update BTD file metadata
@anvil.server.callable
def update_btd_metadata(btd_file_id, metadata):
    """
    Update metadata fields of a BTD file
    
    metadata = {
        'comp_l1': 'NCAA Championship',
        'comp_l2': 'Regional',
        'comp_l3': 'Finals',
        'date': date object,
        'team': 'BEACHINTERNALS',
        'private': True/False
    }
    
    Returns: {
        'success': True/False,
        'message': 'Update successful' or error message
    }
    """
    
    try:
        row = app_tables.btd_files.get_by_id(btd_file_id)
        
        if not row:
            return {'success': False, 'message': 'BTD file not found'}
        
        # Update fields
        if 'comp_l1' in metadata:
            row['comp_l1'] = metadata['comp_l1']
        if 'comp_l2' in metadata:
            row['comp_l2'] = metadata['comp_l2']
        if 'comp_l3' in metadata:
            row['comp_l3'] = metadata['comp_l3']
        if 'date' in metadata:
            row['date'] = metadata['date']
        if 'team' in metadata:
            row['team'] = metadata['team']
        if 'private' in metadata:
            row['private'] = metadata['private']
        
        return {'success': True, 'message': 'Metadata updated successfully'}
        
    except Exception as e:
        return {'success': False, 'message': f'Error updating metadata: {str(e)}'}


# ############ Update player mappings
@anvil.server.callable
def update_player_mappings(btd_file_id, mappings):
    """
    Update player mapping fields
    
    mappings = {
        'player1': 'TEAM1 J.Smith',
        'player2': 'TEAM1 K.Jones',
        'player3': 'TEAM2 L.Brown',
        'player4': 'TEAM2 M.Davis',
        'ppr_playera1': 'TEAM1 10 Smith',
        'ppr_playera2': 'TEAM1 12 Jones',
        'ppr_playerb1': 'TEAM2 5 Brown',
        'ppr_playerb2': 'TEAM2 8 Davis'
    }
    
    Returns: {
        'success': True/False,
        'message': 'Update successful' or error message
    }
    """
    
    try:
        row = app_tables.btd_files.get_by_id(btd_file_id)
        
        if not row:
            return {'success': False, 'message': 'BTD file not found'}
        
        # Update player fields
        if 'player1' in mappings:
            row['player1'] = mappings['player1']
        if 'player2' in mappings:
            row['player2'] = mappings['player2']
        if 'player3' in mappings:
            row['player3'] = mappings['player3']
        if 'player4' in mappings:
            row['player4'] = mappings['player4']
        if 'ppr_playera1' in mappings:
            row['ppr_playera1'] = mappings['ppr_playera1']
        if 'ppr_playera2' in mappings:
            row['ppr_playera2'] = mappings['ppr_playera2']
        if 'ppr_playerb1' in mappings:
            row['ppr_playerb1'] = mappings['ppr_playerb1']
        if 'ppr_playerb2' in mappings:
            row['ppr_playerb2'] = mappings['ppr_playerb2']
        
        return {'success': True, 'message': 'Player mappings updated successfully'}
        
    except Exception as e:
        return {'success': False, 'message': f'Error updating player mappings: {str(e)}'}


# ############ Replace CSV file and update all related fields
@anvil.server.callable
def replace_btd_csv(btd_file_id, new_csv_file):
    """
    Replace the CSV data in a BTD file and recalculate all metrics
    This triggers reprocessing by updating btd_file_date
    
    Returns: {
        'success': True/False,
        'message': 'Update successful' or error message,
        'analysis': {...} from analyze_btd_csv_strict
    }
    """
    
    try:
        row = app_tables.btd_files.get_by_id(btd_file_id)
        
        if not row:
            return {'success': False, 'message': 'BTD file not found', 'analysis': None}
        
        # Analyze the new CSV file
        analysis = analyze_btd_csv_strict(new_csv_file)
        
        if not analysis['valid']:
            return {
                'success': False, 
                'message': f"CSV validation failed: {'; '.join(analysis['errors'])}", 
                'analysis': analysis
            }
        
        # Update the BTD file record
        row['csv_data'] = new_csv_file
        row['filename'] = new_csv_file.name
        row['btd_file_date'] = datetime.datetime.now()  # This triggers reprocessing
        
        # Update metrics
        metrics = analysis['metrics']
        row['points'] = metrics['points']
        row['completeness_score'] = metrics['completeness_score']
        row['per_players'] = metrics['per_players']
        row['per_xy'] = metrics['per_xy']
        row['per_srv_players'] = metrics['per_srv_players']
        
        # Update player list (from BTD file)
        players = analysis['players']
        row['player1'] = players[0] if len(players) > 0 else ''
        row['player2'] = players[1] if len(players) > 1 else ''
        row['player3'] = players[2] if len(players) > 2 else ''
        row['player4'] = players[3] if len(players) > 3 else ''
        
        # Update error tracking
        row['no_errors'] = len(analysis['errors'])
        row['error_str'] = '; '.join(analysis['errors']) if analysis['errors'] else ''
        
        return {
            'success': True, 
            'message': 'CSV file replaced successfully. PPR data will be regenerated overnight.',
            'analysis': analysis
        }
        
    except Exception as e:
        return {
            'success': False, 
            'message': f'Error replacing CSV file: {str(e)}',
            'analysis': None
        }


# ############ Delete a BTD file
@anvil.server.callable
def delete_btd_file(btd_file_id):
    """
    Delete a BTD file from the system
    
    Returns: {
        'success': True/False,
        'message': 'Delete successful' or error message
    }
    """
    
    try:
        row = app_tables.btd_files.get_by_id(btd_file_id)
        
        if not row:
            return {'success': False, 'message': 'BTD file not found'}
        
        filename = row['filename']
        row.delete()
        
        return {'success': True, 'message': f'BTD file "{filename}" deleted successfully'}
        
    except Exception as e:
        return {'success': False, 'message': f'Error deleting BTD file: {str(e)}'}