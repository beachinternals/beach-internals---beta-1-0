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
  #print(f"League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}")
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
    #print('No Rows Found')
    return ["No Rows"], ["No Rows"], ["No Rows"], ["No Stats Found"]

  # somehow, we are getting a column called unamed: 0, so drop taht
  #print(player_data_df.to_dict())
  player_data_df = player_data_df.drop(['Unnamed: 0'], axis = 1 )
  player_stats_df = player_stats_df.drop(['Unnamed: 0'], axis = 1 )
  #print(player_data_df.to_dict())
  
  # limit to player_data table to just this team
  if disp_team != "INTERNALS":
    #print(f" Disp Team,{disp_team}")
    player_data_df = player_data_df[ player_data_df['team'] == disp_team.strip() ]
  
  # replace nan with blanks
  player_data_df = player_data_df.fillna(' ')
  player_stats_df = player_stats_df.fillna('')
  #print(player_data_df)
  #print(player_stats_df)

  # ---------- This may change, but let's make a subest data set a few columns for this display
  disp_df = player_data_df[['team','player','fbhe','fbhe1', 'fbhe2', 'fbhe3', 'fbhe4', 'fbhe5']]
  mean_df1_dict = {'team':['Mean:'],
                         'player':[' '],
                         'fbhe':[disp_df['fbhe'].mean(skipna=True)],
                         'fbhe1':[disp_df['fbhe1'].mean(skipna=True)],
                         'fbhe2':[disp_df['fbhe2'].mean(skipna=True)],
                         'fbhe3':[disp_df['fbhe3'].mean(skipna=True)],
                         'fbhe4':[disp_df['fbhe4'].mean(skipna=True)],
                         'fbhe5':[disp_df['fbhe5'].mean(skipna=True)] }
  mean_df1 = pd.DataFrame.from_dict(mean_df1_dict)
  sd_df1_dict = {'team':['St Dev:'],
                         'player':[' '],
                         'fbhe':[disp_df['fbhe'].std(skipna=True)],
                         'fbhe1':[disp_df['fbhe1'].std(skipna=True)],
                         'fbhe2':[disp_df['fbhe2'].std(skipna=True)],
                         'fbhe3':[disp_df['fbhe3'].std(skipna=True)],
                         'fbhe4':[disp_df['fbhe4'].std(skipna=True)],
                         'fbhe5':[disp_df['fbhe5'].std(skipna=True)] }
  sd_df1 = pd.DataFrame.from_dict(sd_df1_dict)
  disp_df = pd.concat([disp_df,mean_df1,sd_df1])
  df_table1 = pd.DataFrame.to_markdown(disp_df, index=False )

  disp_df = player_data_df[['player','srv1_fbhe','srv3_fbhe','srv5_fbhe']]
  mean_df2_dict = {'player':['Mean:'],
                         'srv1_fbhe':[disp_df['srv1_fbhe'].mean(skipna=True)],
                         'srv3_fbhe':[disp_df['srv3_fbhe'].mean(skipna=True)],
                         'srv5_fbhe':[disp_df['srv5_fbhe'].mean(skipna=True)] }
  mean_df2 = pd.DataFrame.from_dict(mean_df2_dict)
  sd_df2_dict = {'player':['St Dev:'],
                         'srv1_fbhe':[disp_df['srv1_fbhe'].std(skipna=True)],
                         'srv3_fbhe':[disp_df['srv3_fbhe'].std(skipna=True)],
                         'srv5_fbhe':[disp_df['srv5_fbhe'].std(skipna=True)] }
  sd_df2 = pd.DataFrame.from_dict(sd_df2_dict)
  disp_df = pd.concat([disp_df,mean_df2,sd_df2])
  df_table2 = pd.DataFrame.to_markdown(disp_df, index=False )
  
  disp_df = player_data_df[['player','err_den','tcr','tcr_r','tcr_s','expected']]
  mean_df3_dict = {'player':['Mean:'],
                         'err_den':[disp_df['err_den'].mean(skipna=True)],
                         'tcr':[disp_df['tcr'].mean(skipna=True)],
                         'tcr_r':[disp_df['tcr_r'].mean(skipna=True)],
                         'tcr_s':[disp_df['tcr_s'].mean(skipna=True)],
                         'expected':[disp_df['expected'].mean(skipna=True)] }
  mean_df3 = pd.DataFrame.from_dict(mean_df3_dict)
  sd_df3_dict = {'player':['St Dev:'],
                         'err_den':[disp_df['err_den'].std(skipna=True)],
                         'tcr':[disp_df['tcr'].std(skipna=True)],
                         'tcr_r':[disp_df['tcr_r'].std(skipna=True)],
                         'tcr_s':[disp_df['tcr_s'].std(skipna=True)],
                         'expected':[disp_df['expected'].std(skipna=True)] }
  sd_df3 = pd.DataFrame.from_dict(sd_df3_dict)
  disp_df = pd.concat([disp_df,mean_df3,sd_df3])
  df_table3 = pd.DataFrame.to_markdown(disp_df, index=False )

  player_stats_disp = player_stats_df[['fbhe_mean','fbhe_range_mean','err_den_mean','tcr_mean','tcr_r_mean','tcr_s_mean','expected_mean']]
  df_stats_table = pd.DataFrame.to_markdown(player_stats_disp, index=False)
  
  return df_table1, df_table2, df_table3, df_stats_table