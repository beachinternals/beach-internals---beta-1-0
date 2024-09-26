import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import pandas as pd
import io
from tabulate import tabulate


# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

@anvil.server.callable
def coaches_dashboard(league_value, disp_team):

  # unpack the league value
  str_loc = league_value.index('|')
  disp_league = league_value[:str_loc-1].strip()
  league_value = league_value[str_loc+1:]
  str_loc = league_value.index('|')
  disp_gender = league_value[:str_loc-1].strip()
  disp_year = league_value[str_loc+1:].strip()
  
  # find the play_data table
  # pull out the player_data csv file
  print(f"League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}")
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = "League"
      ) )

  if ppr_csv_row:
    player_data_df =  pd.read_csv(io.BytesIO( ppr_csv_row['player_data'].get_bytes()))
    #player_stats_df =  pd.read_csv(io.BytesIO( ppr_csv_row['player_data_stats'].get_bytes()))
  else:
    print('No Rows Found')
    return ["No Rows"]
    
  # limit to player_data table to just this team
  #print(f"Playerdf player {player_data_df['player']}")
  #print(f"Playerdf player 0 {player_data_df.at[0,'player']}")
  #print(f"Playerdf player 0, 0-3 {player_data_df.at[0,'player'][0:3]}")
  #print(f"Playerdf player, 0-3 {player_data_df['player'][0:3]}")
  #print(f"disp team o-3:{disp_team[0:3]}")
  #player_data_df = player_data_df[player_data_df['player'][0:3] == disp_team][0:3]

  # convert df to markdown table
  df_table = pd.DataFrame.to_markdown(player_data_df)
  
  return df_table