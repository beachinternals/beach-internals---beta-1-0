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
    player_stats_df =  pd.read_csv(io.BytesIO( ppr_csv_row['player_data_stats'].get_bytes()))
  else:
    print('No Rows Found')
    return ["No Rows"]

  # somehow, we are getting a column called unamed: 0, so drop taht
  #print(player_data_df.to_dict())
  player_data_df = player_data_df.drop(['Unnamed: 0'], axis = 1 )
  player_stats_df = player_stats_df.drop(['Unnamed: 0'], axis = 1 )
  #print(player_data_df.to_dict())
  
  # limit to player_data table to just this team
  if disp_team != "INTERNALS":
    print(f" Disp Team,{disp_team}")
    player_data_df = player_data_df[ player_data_df['team'] == disp_team.strip() ]
  
  # replace nan with blanks
  player_data_df = player_data_df.fillna(' ')
  player_stats_df = player_stats_df.fillna('')
  print(player_data_df)
  #print(player_stats_df)

  # now, limit the data to this team, disp_team
  #player_data_df = player_data_df[player_data_df['team'] == disp_team]

  # convert df to markdown table
  df_table = pd.DataFrame.to_markdown(player_data_df, index=False )
  df_stats_table = pd.DataFrame.to_markdown(player_stats_df, index=False )
  return df_table, df_stats_table