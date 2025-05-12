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
import math
from tabulate import tabulate
from server_functions import *


# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

#-----------------------------------------------
#
#       Create the coaches dashboard (and other dashboard) reports
#
#-------------------------------------------------
@anvil.server.callable
def coaches_dashboard(disp_league, disp_gender, disp_year, 
                                   disp_team, disp_pair, disp_player, 
                                   comp_l1_checked, comp_l1,
                                   comp_l2_check_box, comp_l2,
                                   comp_l3_check_box, comp_l3,
                                   date_check_box, start_date, end_date,
                                   scout, explain_text ):

  # unpack the league value
  #disp_league, disp_gender, disp_year = unpack_league(league_value)

  # fetch the player data and stats dataframes
  player_data_df, player_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  
  # limit to player_data table to just this team
  if ( disp_team != "INTERNALS"):
    if (disp_league.strip() != "FIVB" and disp_year.strip() != "2024"):
      print(f" Disp Team,{disp_team}, disp league: {disp_league}, disp_year: {disp_year}")
      player_data_df = player_data_df[ player_data_df['team'] == disp_team.strip() ]

  # ---------- This may change, but let's make a subest data set a few columns for this display
  disp_df = player_data_df[['team','player','fbhe','fbhe1', 'fbhe2', 'fbhe3', 'fbhe4', 'fbhe5']]
  mean_df1_dict = {'team':['Mean:'],
                         'player':[' '],
                         'fbhe':["{:.3f}".format(disp_df['fbhe'].mean(skipna=True, numeric_only=True))],
                         'fbhe1':["{:.3f}".format(disp_df['fbhe1'].mean(skipna=True, numeric_only=True))],
                         'fbhe2':["{:.3f}".format(disp_df['fbhe2'].mean(skipna=True, numeric_only=True))],
                         'fbhe3':["{:.3f}".format(disp_df['fbhe3'].mean(skipna=True, numeric_only=True))],
                         'fbhe4':["{:.3f}".format(disp_df['fbhe4'].mean(skipna=True, numeric_only=True))],
                         'fbhe5':["{:.3f}".format(disp_df['fbhe5'].mean(skipna=True, numeric_only=True))] }
  mean_df1 = pd.DataFrame.from_dict(mean_df1_dict)
  sd_df1_dict = {'team':['St Dev:'],
                         'player':[' '],
                         'fbhe':["{:.3f}".format(disp_df['fbhe'].std(skipna=True))],
                         'fbhe1':["{:.3f}".format(disp_df['fbhe1'].std(skipna=True))],
                         'fbhe2':["{:.3f}".format(disp_df['fbhe2'].std(skipna=True))],
                         'fbhe3':["{:.3f}".format(disp_df['fbhe3'].std(skipna=True))],
                         'fbhe4':["{:.3f}".format(disp_df['fbhe4'].std(skipna=True))],
                         'fbhe5':["{:.3f}".format(disp_df['fbhe5'].std(skipna=True))] }
  sd_df1 = pd.DataFrame.from_dict(sd_df1_dict)
  
  disp_df = pd.concat([disp_df.fillna(''),mean_df1,sd_df1])
  df_table1 = pd.DataFrame.to_markdown(disp_df, index=False )

  disp_df = player_data_df[['player','srv1_fbhe','srv3_fbhe','srv5_fbhe']]
  mean_df2_dict = {'player':['Mean:'],
                         'srv1_fbhe':["{:.3f}".format(disp_df['srv1_fbhe'].mean(skipna=True))],
                         'srv3_fbhe':["{:.3f}".format(disp_df['srv3_fbhe'].mean(skipna=True))],
                         'srv5_fbhe':["{:.3f}".format(disp_df['srv5_fbhe'].mean(skipna=True))] }
  mean_df2 = pd.DataFrame.from_dict(mean_df2_dict)
  sd_df2_dict = {'player':['St Dev:'],
                         'srv1_fbhe':["{:.3f}".format(disp_df['srv1_fbhe'].std(skipna=True))],
                         'srv3_fbhe':["{:.3f}".format(disp_df['srv3_fbhe'].std(skipna=True))],
                         'srv5_fbhe':["{:.3f}".format(disp_df['srv5_fbhe'].std(skipna=True))] }
  sd_df2 = pd.DataFrame.from_dict(sd_df2_dict)
  disp_df = pd.concat([disp_df.fillna(''),mean_df2,sd_df2])
  df_table2 = pd.DataFrame.to_markdown(disp_df, index=False )
  
  disp_df = player_data_df[['player','err_den','tcr','tcr_r','tcr_s','expected']]
  mean_df3_dict = {'player':['Mean:'],
                         'err_den':["{:.1f}".format(disp_df['err_den'].mean(skipna=True))],
                         'tcr':["{:.1f}".format(disp_df['tcr'].mean(skipna=True))],
                         'tcr_r':["{:.1f}".format(disp_df['tcr_r'].mean(skipna=True))],
                         'tcr_s':["{:.1f}".format(disp_df['tcr_s'].mean(skipna=True))],
                         'expected':["{:.1f}".format(disp_df['expected'].mean(skipna=True))] }
  mean_df3 = pd.DataFrame.from_dict(mean_df3_dict)
  sd_df3_dict = {'player':['St Dev:'],
                         'err_den':["{:.2f}".format(disp_df['err_den'].std(skipna=True))],
                         'tcr':["{:.2f}".format(disp_df['tcr'].std(skipna=True))],
                         'tcr_r':["{:.2f}".format(disp_df['tcr_r'].std(skipna=True))],
                         'tcr_s':["{:.2f}".format(disp_df['tcr_s'].std(skipna=True))],
                         'expected':["{:.2f}".format(disp_df['expected'].std(skipna=True))] }
  sd_df3 = pd.DataFrame.from_dict(sd_df3_dict)
  disp_df = pd.concat([disp_df.fillna(''),mean_df3,sd_df3])
  df_table3 = pd.DataFrame.to_markdown(disp_df, index=False )

  player_stats_disp = {
    'Value':['Mean','St Dev'],
    'FBHE':["{:.3f}".format(player_stats_df.at[0,'fbhe_mean']),"{:.3f}".format(player_stats_df.at[0,'fbhe_stdev'])] ,
#    'FBHE - From 1':["{:.3f}".format(player_stats_df.at[0,'srv1_fbhe_mean']),"{:.3f}".format(player_stats_df.at[0,'srv1_fbhe_stdev'])] ,
#    'FBHE - From 3':["{:.3f}".format(player_stats_df.at[0,'srv3_fbhe_mean']),"{:.3f}".format(player_stats_df.at[0,'srv3_fbhe_stdev'])] ,
#    'FBHE - From 5':["{:.3f}".format(player_stats_df.at[0,'srv5_fbhe_mean']),"{:.3f}".format(player_stats_df.at[0,'srv5_fbhe_stdev'])] ,
    'Expected Value':["{:.1f}".format(player_stats_df.at[0,'expected_mean']),"{:.1f}".format(player_stats_df.at[0,'expected_stdev'])],
    'Err Den':["{:.1f}".format(player_stats_df.at[0,'err_den_mean']),"{:.1f}".format(player_stats_df.at[0,'err_den_stdev'])] ,
    'Transition Conversion':["{:.1f}".format(player_stats_df.at[0,'tcr_mean']),"{:.1f}".format(player_stats_df.at[0,'tcr_stdev'])] ,
    'TCR - Serving':["{:.1f}".format(player_stats_df.at[0,'tcr_s_mean']),"{:.1f}".format(player_stats_df.at[0,'tcr_s_stdev'])] ,
    'TCR - Receiving':["{:.1f}".format(player_stats_df.at[0,'tcr_r_mean']),"{:.1f}".format(player_stats_df.at[0,'tcr_r_stdev'])] 

  }
  df_stats_table = pd.DataFrame.to_markdown(player_stats_disp, index=False)
  
  return df_table1, df_table2, df_table3

'''
Season Timeline report

Identify key metrics for you whole team broken down by week

'''
@anvil.server.callable
def coaches_dashboard_season_summary(disp_league, disp_gender, disp_year, 
                        disp_team, disp_pair, disp_player, 
                        comp_l1_checked, comp_l1,
                        comp_l2_check_box, comp_l2,
                        comp_l3_check_box, comp_l3,
                        date_check_box, start_date, end_date,
                        scout, explain_text ):

  # date range passed as start data and end data, then we report on start date +7 until we reach end data
  # reporting on all players in disp_team

  # get the ppr data
  ppr_df = get_ppr_data(disp_league,disp_gender,disp_year, disp_team)
  
  # filter it by the filters given
  ppr_df = ppr_df_limit( comp_l1_checked, comp_l1,
                         comp_l2_check_box, comp_l2,
                         comp_l3_check_box, comp_l3,
                         date_check_box, start_date, end_date
                       )

  # filter it to just this team
  disp_team = disp_team.strip()
  ppr_df = ppr_df[ ( ppr_df['player_a1'][:len(disp_team)].strip() == disp_team | ppr_df['player_b1'][:len(disp_team)].strip() == disp_team ) ]

  # define data frame to be returned (as a mkdn file)
  # we want the variables in rows, the weeks time) in columns

  # calculate the time
  no_days = end_date - start_date
  no_weeks = math.ciel(no_days/7)
  if no_weeks < 1:
    print(f"Error: no weeks between end date and start date, end date: {end_date}, Start Date: {start_date}")
    return 'Error in Dates', '', ''
    
  '''
  Variables to be calculated in columns

  FBHE
  % in system passing
  Knock Out serving
  points percentage
  erro den
  tcr
  rows in data
  
  '''
  dash_dict = {
    'Week':['1'],
    'FBHE':[0],
    '% in Sys':[0],
    '% Knock Out':[0],
    '% Pts Earned':[0],
    'Error Density':[0],
    'Transiton Conversion':[0],
    'Points in Data':[0]
  }
  dash_df = pd.DataFrame.from_dict( dash_dict )
  
  # loop over weeks
  for wk in range[1,no_weeks]:
    begin_date = start_date + 7*(wk-1)
    stop_date = begin_date + 7
    tmp_df = ppr_df[ ( ppr_df['date'] >= begin_date & ppr_df['date'] <= stop_date ) ]

    # ow assign the variables:
    dash_df.at[wk-1,'Points in Data'] = tmp_df.shape[0]
    
    

  # convert dataframe to markdown

  # return
  return