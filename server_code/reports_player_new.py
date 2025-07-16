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
import math
import inspect
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import numpy as np
import scipy.stats as stats
from tabulate import tabulate
from server_functions import *
from anvil import pdf
from pair_functions import *
from matchup_reports import player_45_serves
from plot_functions import *
from datetime import datetime, timedelta
from server_functions import *
from plot_functions import *

'''

List of player reports

All 'new' format using opoup window and filters by **rpt_filters

'''

#---------------------------------------------------------------------------
#
#              player report stub NEW
#
#---------------------------------------------------------------------------
@anvil.server.callable
def report_player_stub_new(lgy, team, **rpt_filters):
  '''
  Report Functions:

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)

  # initiate return lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=inspect.currentframe().f_code.co_name)
  title_list[0] = rpt_row['rpt_title']
  title_list[1] = rpt_row['rpt_sub_title']
  title_list[2] = rpt_row['rpt_section_title1']
  title_list[3] = rpt_row['lgy']
  title_list[4] = rpt_row['team_name']
  title_list[5] = rpt_row['rpt_type']
  title_list[6] = rpt_row['filter_text']
  title_list[7] = rpt_row['explain_text']
  title_list[8] = rpt_filters.get('player')
  title_list[9]= rpt_filters.get('pair')

  label_list[0] = rpt_row['box1_title']
  label_list[1] = rpt_row['box2_title']
  label_list[2] = rpt_row['box3_title']
  label_list[3] = rpt_row['box4_title']
  label_list[4] = rpt_row['box5_title']
  label_list[5] = rpt_row['box6_title']
  label_list[6] = rpt_row['box7_title']
  label_list[7] = rpt_row['box8_title']
  label_list[8] = rpt_row['box9_title']
  label_list[9] = rpt_row['box10_title']

  #------------------------------------------------------------------------------------------------------
  #
  #            Create the images and dataframes with filtered ppr data for report
  #
  #-----------------------------------------------------------------------------------------------------

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]

  # set up dataframe 1 to display the attack table for this player
  att_table = get_player_attack_table(ppr_df, disp_player)
  df_list[0] = att_table.to_dict('records')

  # get the grpahs of attacks, zone 1 - 5, all as one graph
  z1_plt, z2_plt, z3_plt, z4_plt, z5_plt, z1_df, z2_df, z3_df, z4_df, z5_df = get_player_attack_plots(ppr_df, disp_player)

  # put the plots in the image list
  image_list[0] = z1_plt
  image_list[1] = z2_plt
  image_list[2] = z3_plt
  image_list[3] = z4_plt
  image_list[4] = z5_plt

  # put the DF's in the df_list
  df_list[1] = z1_df.to_dict('records')
  df_list[2] = z2_df.to_dict('records')
  df_list[3] = z3_df.to_dict('records')
  df_list[4] = z4_df.to_dict('records')
  df_list[5] = z5_df.to_dict('records')


  return title_list, label_list, image_list, df_list


  
#---------------------------------------------------------------------------------------------------
#
#.  Player Consistency Report
#
#-------------------------------------------------------------------------------------------------
@anvil.server.callable
def  player_consistency_report_new(lgy, team, **rpt_filters):
  '''

  Custom, per report code:
  - set the True/False for pair or player
  - create and store dataframes
  - create and store images
  
  Report Functions:

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  title_list, label_list, image_list, df_list = initialize_report_lists(inspect.currentframe().f_code.co_name, **rpt_filters)

  #------------------------------------------------------------------------------------------------------
  #            Set ot a Player or Pair Report
  #------------------------------------------------------------------------------------------------------
  disp_player = rpt_filters.get('player')
  disp_pair = rpt_filters.get('pair')
  # for a player report:
  if True:  # set only one of these to True
    ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
      (ppr_df['player_a2'] == disp_player) |
      (ppr_df['player_b1'] == disp_player) |
      (ppr_df['player_b2'] == disp_player) 
      ]
  # for a pair report:
  if False:  # set only one of these to True
    ppr_df = ppr_df[ (ppr_df['teama'] == disp_pair) | (ppr_df['teamb'] == disp_pair) ]

  #------------------------------------------------------------------------------------------------------
  #            Create and store DataFrames
  #------------------------------------------------------------------------------------------------------
  cons_table, no_data = calc_consistency_match_table( ppr_df, disp_player )
  # now calculate percentile
  cons_table.at[9,'FBHE'] = 1 - stats.norm.cdf( (cons_table.at[8,'FBHE'] - player_data_stats_df.at[0,'cons_fbhe_sd_match_mean'])/ player_data_stats_df.at[0,'cons_fbhe_sd_match_stdev'] )
  cons_table.at[9,'Error Den'] = 1 - stats.norm.cdf( (cons_table.at[8,'Error Den'] - player_data_stats_df.at[0,'cons_ed_sd_match_mean'])/ player_data_stats_df.at[0,'cons_ed_sd_match_stdev'] )
  cons_table.at[9,'Tran Conv'] = 1 - stats.norm.cdf( (cons_table.at[8,'Tran Conv'] - player_data_stats_df.at[0,'cons_tcr_sd_match_mean'])/ player_data_stats_df.at[0,'cons_tcr_sd_match_stdev'] )
  cons_table.at[9,'Knockout %'] = 1 - stats.norm.cdf( (cons_table.at[8,'Knockout %'] - player_data_stats_df.at[0,'cons_ko_sd_match_mean'])/ player_data_stats_df.at[0,'cons_ko_sd_match_stdev'] )
  cons_table.at[9,'Good Passes'] = 1 - stats.norm.cdf( (cons_table.at[8,'Good Passes'] - player_data_stats_df.at[0,'cons_pass_sd_match_mean'])/ player_data_stats_df.at[0,'cons_pass_sd_match_stdev'] )
  cons_table.at[9,'Points Earned'] = 1 - stats.norm.cdf( (cons_table.at[8,'Points Earned'] - player_data_stats_df.at[0,'cons_pts_sd_match_mean'])/ player_data_stats_df.at[0,'cons_pts_sd_match_stdev'] )
  cons_table.at[9,'Error Den'] = str('{:.1%}').format(cons_table.at[9,'Error Den'])
  cons_table.at[9,'Tran Conv'] = str('{:.1%}').format(cons_table.at[9,'Tran Conv'])
  cons_table.at[9,'Knockout %'] = str('{:.1%}').format(cons_table.at[9,'Knockout %'])
  cons_table.at[9,'Good Passes'] = str('{:.1%}').format(cons_table.at[9,'Good Passes'])
  cons_table.at[9,'Points Earned'] = str('{:.1%}').format(cons_table.at[9,'Points Earned'])
  cons_table.at[9,'FBHE'] = str('{:.1%}').format(cons_table.at[9,'FBHE'])
  cons_table.at[9,'Att'] = ''
  cons_table.at[9,'Points'] = ''

  # Define desired column order
  column_order = [' ', 'Points', 'Att', 'FBHE', 'Tran Conv', 'Error Den', 'Knockout %', 'Good Passes', 'Points Earned']
  cons_table = cons_table.reindex(columns = column_order)
  df_list[0] = cons_table.to_dict('records')
  
  cons2_table, no_data1 = calc_consistency_s2s_table( ppr_df, disp_player )
  # now calculate percentile
  index = cons2_table.shape[0]
  cons2_table.at[index,'Set'] = 'Percentile'
  cons2_table.at[index,'FBHE'] = 1 - stats.norm.cdf( (cons2_table.at[index-1,'FBHE'] - player_data_stats_df.at[0,'cons_fbhe_sd_s2s_mean'])/ player_data_stats_df.at[0,'cons_fbhe_sd_s2s_stdev'] )
  cons2_table.at[index,'Error Den'] = 1 - stats.norm.cdf( (cons2_table.at[index-1,'Error Den'] - player_data_stats_df.at[0,'cons_ed_sd_s2s_mean'])/ player_data_stats_df.at[0,'cons_ed_sd_s2s_stdev'] )
  cons2_table.at[index,'Tran Conv'] = 1 - stats.norm.cdf( (cons2_table.at[index-1,'Tran Conv'] - player_data_stats_df.at[0,'cons_tcr_sd_s2s_mean'])/ player_data_stats_df.at[0,'cons_tcr_sd_s2s_stdev'] )
  cons2_table.at[index,'Knockout %'] = 1 - stats.norm.cdf( (cons2_table.at[index-1,'Knockout %'] - player_data_stats_df.at[0,'cons_ko_sd_s2s_mean'])/ player_data_stats_df.at[0,'cons_ko_sd_s2s_stdev'] )
  cons2_table.at[index,'Good Passes'] = 1 - stats.norm.cdf( (cons2_table.at[index-1,'Good Passes'] - player_data_stats_df.at[0,'cons_pass_sd_s2s_mean'])/ player_data_stats_df.at[0,'cons_pass_sd_s2s_stdev'] )
  cons2_table.at[index,'Points Earned'] = 1 - stats.norm.cdf( (cons2_table.at[index-1,'Points Earned'] - player_data_stats_df.at[0,'cons_pts_sd_s2s_mean'])/ player_data_stats_df.at[0,'cons_pts_sd_s2s_stdev'] )
  cons2_table.at[index,'Att'] = ''
  cons2_table.at[index,'Points'] = ''
  cons2_table.at[index,'FBHE'] = str('{:.1%}').format(cons2_table.at[index,'FBHE'])
  cons2_table.at[index,'Error Den'] = str('{:.1%}').format(cons2_table.at[index,'Error Den'])
  cons2_table.at[index,'Tran Conv'] = str('{:.1%}').format(cons2_table.at[index,'Tran Conv'])
  cons2_table.at[index,'Knockout %'] = str('{:.1%}').format(cons2_table.at[index,'Knockout %'])
  cons2_table.at[index,'Good Passes'] = str('{:.1%}').format(cons2_table.at[index,'Good Passes'])
  cons2_table.at[index,'Points Earned'] = str('{:.1%}').format(cons2_table.at[index,'Points Earned'])

  # Define desired column order
  column_order = ['Set', 'Points', 'Att', 'FBHE', 'Tran Conv', 'Error Den', 'Knockout %', 'Good Passes', 'Points Earned']
  cons2_table = cons2_table.reindex(columns = column_order)
  
  df_list[1] = cons2_table.to_dict('records')

  #------------------------------------------------------------------------------------------------------
  #            Create and store images
  #------------------------------------------------------------------------------------------------------
  # z1_plt = get_player_attack_plots(ppr_df, disp_player)
  # image_list[0] = z1_plt

  return title_list, label_list, image_list, df_list

#---------------------------------------------------------------------------------------------------
#
#.  Player Season Summary
#
#-------------------------------------------------------------------------------------------------
@anvil.server.callable
def  player_season_summary_new(lgy, team, **rpt_filters):
  '''

  Custom, per report code:
  - set the True/False for pair or player
  - create and store dataframes
  - create and store images
  
  Report Functions:

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  title_list, label_list, image_list, df_list = initialize_report_lists(inspect.currentframe().f_code.co_name, **rpt_filters)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)

  #------------------------------------------------------------------------------------------------------
  #            Set ot a Player or Pair Report
  #------------------------------------------------------------------------------------------------------
  disp_player = rpt_filters.get('player')
  disp_pair = rpt_filters.get('pair')
  # for a player report:
  if True:  # set only one of these to True
    ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
      (ppr_df['player_a2'] == disp_player) |
      (ppr_df['player_b1'] == disp_player) |
      (ppr_df['player_b2'] == disp_player) 
      ]
  # for a pair report:
  if False:  # set only one of these to True
    ppr_df = ppr_df[ (ppr_df['teama'] == disp_pair) | (ppr_df['teamb'] == disp_pair) ]

  # for a team report
  if False:
    ppr_df = ppr_df[ ppr_df['teama'] == disp_pair ]

  #------------------------------------------------------------------------------------------------------
  #            Create and store DataFrames
  #------------------------------------------------------------------------------------------------------
  # no dataframes in this report

  #------------------------------------------------------------------------------------------------------
  #            Create and store images
  #------------------------------------------------------------------------------------------------------

  # create a list with the dates in question
  num_weeks = 11
  if 'start_date' in rpt_filters:
    start_date = rpt_filters.get('start_date')
  else:
    start_date = datetime(2025, 2, 19)
    
  if 'end_date' in rpt_filters:
    end_date = rpt_filters.get('end_date')
    num_weeks = (end_date - start_date)/7
  else:
    end_date = start_date + timedelta(days=7*num_weeks)
    
  # create a list with the start and end date for each week
  weekly_dates = []
  for i in range(num_weeks):
    week_start = start_date + timedelta(days=7 * i)
    week_end = week_start + timedelta(days=6)
    weekly_dates.append({
      'start_date': week_start,
      'end_date': week_end
    })

  # set up the pandas dataframe
  df_dict = {'Variable':['Week 1','Week 2','Week 3','Week 4','Week 5','Week 6','Week 7','Week 8','Week 9','Week 10', 'Week 11'],
               'FBHE':[0,0,0,0,0,0,0,0,0,0,0],
               'Errors':[0,0,0,0,0,0,0,0,0,0,0],
               'Transition':[0,0,0,0,0,0,0,0,0,0,0],
               'Knockout':[0,0,0,0,0,0,0,0,0,0,0],
               'Good Pass':[0,0,0,0,0,0,0,0,0,0,0],
               'Points':[0,0,0,0,0,0,0,0,0,0,0]
    }
  sum_df = pd.DataFrame.from_dict(df_dict)

  #print(sum_df)
  # start a loop over the weeks
  for i in range(0,num_weeks):
    week_var = 'Week '+str(i+1)
    # filter ppr_df to tmp_df for this week
    #print(f"type of ppr_df game_date: {type(ppr_df['game_date'])}")
    #print(f"weekly dates : {type(weekly_dates)}, [i] {type(weekly_dates[i])}, [i][start_date] {type(weekly_dates[i]['start_date'])}")
    #print(f"weekly dates : {weekly_dates}, [i] {weekly_dates[i]}, [i][start_date] {weekly_dates[i]['start_date']}")

    tmp_df = ppr_df
    tmp_df['game_date'] = pd.to_datetime(tmp_df['game_date'])
    tmp_df['game_date'] = tmp_df['game_date'].dt.date
    #m_ppr_df = m_ppr_df.loc[(m_ppr_df['game_date'] >= disp_start_date) & (m_ppr_df['game_date'] <= disp_end_date) ]
    #print(f"Limitiing by Dates:{disp_start_date},{disp_end_date}")
    
    tmp_df = tmp_df[ (tmp_df['game_date'] >= weekly_dates[i]['start_date'].date()) & (tmp_df['game_date'] < weekly_dates[i]['end_date'].date()) ]

    #print(f"week : {i}, week var: {week_var}, start date: {weekly_dates[i]['start_date'].date()}, end date: {weekly_dates[i]['end_date'].date()}, number of points: {tmp_df.shape[0]}")
    
    pt_totals_df = player_pt_total( tmp_df, disp_player )
    sum_df.loc[i,'FBHE'] = (pt_totals_df.at[0,'p_fbk']-pt_totals_df.at[0,'p_fbe'])/( pt_totals_df.at[0,'p_att_total'])
    sum_df.loc[i,'Errors' ] = (pt_totals_df.at[0,'p_fbe']+pt_totals_df.at[0,'p_tse']+pt_totals_df.at[0,'p_te_r']+pt_totals_df.at[0,'p_te_s'])/( pt_totals_df.at[0,'pts_total'])
    sum_df.loc[i,'Transition' ] = (pt_totals_df.at[0,'p_tk_s']+pt_totals_df.at[0,'p_tk_r']+pt_totals_df.at[0,'o_te_r']+pt_totals_df.at[0,'o_te_s'])/( pt_totals_df.at[0,'trans_total'])
    sum_df.loc[i,'Knockout'] = (pt_totals_df.at[0,'p_tsa']+pt_totals_df.at[0,'o_bad_pass'])/( pt_totals_df.at[0,'p_serves'])
    sum_df.loc[i,'Good Pass'] = (pt_totals_df.at[0,'p_good_pass'])/( pt_totals_df.at[0,'p_good_pass']+pt_totals_df.at[0,'p_bad_pass'])
    sum_df.loc[i,'Points' ] = ( (pt_totals_df.at[0,'p_tsa']+pt_totals_df.at[0,'p_fbk']+pt_totals_df.at[0,'p_tk_r']+pt_totals_df.at[0,'p_tk_s']) +
                                    (pt_totals_df.at[0,'o_tse']+pt_totals_df.at[0,'o_fbe']+pt_totals_df.at[0,'o_te_r']+pt_totals_df.at[0,'o_te_s']) ) / ( pt_totals_df.at[0,'pts_total']
                                      )
    # format the entries
    sum_df.loc[i,'FBHE'] = "{:.3f}".format(sum_df.loc[i,'FBHE'])
    sum_df.loc[i,'Errors'] = "{:.3f}".format(sum_df.loc[i,'Errors'])
    sum_df.loc[i,'Transition'] = "{:.3f}".format(sum_df.loc[i,'Transition'])
    sum_df.loc[i,'Knockout'] = "{:.3f}".format(sum_df.loc[i,'Knockout'])
    sum_df.loc[i,'Good Pass'] = "{:.3f}".format(sum_df.loc[i,'Good Pass'])
    sum_df.loc[i,'Points'] = "{:.3f}".format(sum_df.loc[i,'Points'])
    
  #print(f" Summary dataframe: \n {sum_df}")
  
  # now create histograms for each one
  size = [11,5]
  avg_title = disp_league + " Average : "
  plt1 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['FBHE'].tolist(), 'First Ball Hitting Efficiency', '', 'FBHE', size, avg_title, player_data_stats_df.at[0,'fbhe_mean'],False,'',''   )
  plt2 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Errors'].tolist(), 'Error Denisty', '', 'Error Denisty', size, avg_title, player_data_stats_df.at[0,'err_den_mean']/100,False,'','' )
  plt3 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Transition'].tolist(), 'Transition Conversion', '', 'Transition Conversion', size, avg_title, player_data_stats_df.at[0,'tcr_mean']/100,False,'','' )
  plt4 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Knockout'].tolist(), 'Serving Aggressiveness', '', 'Serving - Knockout Percent', size, avg_title, player_data_stats_df.at[0,'knockout_mean'],False,'','' )
  plt5 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Good Pass'].tolist(), 'Passing Quality', '', 'Percent Good Passes', size, avg_title, player_data_stats_df.at[0,'goodpass_mean'],False,'','' )
  plt6 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Points'].tolist(), 'Percent of Points Won', '', 'Percent of Points Earned', size, '', 0,False,'','' )

  # store the images in the list
  image_list[0] = plt1
  image_list[1] = plt2
  image_list[2] = plt3
  image_list[3] = plt4
  image_list[4] = plt5
  image_list[5] = plt6
  
  return title_list, label_list, image_list, df_list

#---------------------------------------------------------------------------------------------------
#
#.  Player Season Summary
#
#-------------------------------------------------------------------------------------------------
@anvil.server.callable
def  pair_season_summary_new(lgy, team, **rpt_filters):
  '''

  Custom, per report code:
  - set the True/False for pair or player
  - create and store dataframes
  - create and store images
  
  Report Functions:

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  title_list, label_list, image_list, df_list = initialize_report_lists(inspect.currentframe().f_code.co_name, **rpt_filters)
  pair_data_df, pair_data_stats_df = get_pair_data(disp_league, disp_gender, disp_year)

  #------------------------------------------------------------------------------------------------------
  #            Set ot a Player or Pair Report
  #------------------------------------------------------------------------------------------------------
  disp_player = rpt_filters.get('player')
  disp_pair = rpt_filters.get('pair')
  # for a player report:
  if False:  # set only one of these to True
    ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
      (ppr_df['player_a2'] == disp_player) |
      (ppr_df['player_b1'] == disp_player) |
      (ppr_df['player_b2'] == disp_player) 
      ]
  # for a pair report:
  if True:  # set only one of these to True
    ppr_df = ppr_df[ (ppr_df['teama'] == disp_pair) | (ppr_df['teamb'] == disp_pair) ]

  # for a team report
  if False:
    ppr_df = ppr_df[ ppr_df['teama'] == disp_pair ]

  #------------------------------------------------------------------------------------------------------
  #            Create and store DataFrames
  #------------------------------------------------------------------------------------------------------
  # no dataframes in this report

  #------------------------------------------------------------------------------------------------------
  #            Create and store images
  #------------------------------------------------------------------------------------------------------

  # create a list with the dates in question
  num_weeks = 11
  if 'start_date' in rpt_filters:
    start_date = rpt_filters.get('start_date')
  else:
    start_date = datetime(2025, 2, 19)

  if 'end_date' in rpt_filters:
    end_date = rpt_filters.get('end_date')
    num_weeks = (end_date - start_date)/7
  else:
    end_date = start_date + timedelta(days=7*num_weeks)

  # create a list with the start and end date for each week
  weekly_dates = []
  for i in range(num_weeks):
    week_start = start_date + timedelta(days=7 * i)
    week_end = week_start + timedelta(days=6)
    weekly_dates.append({
      'start_date': week_start,
      'end_date': week_end
    })

  # set up the pandas dataframe
  df_dict = {'Variable':['Week 1','Week 2','Week 3','Week 4','Week 5','Week 6','Week 7','Week 8','Week 9','Week 10', 'Week 11'],
             'FBHE':[0,0,0,0,0,0,0,0,0,0,0],
             'Errors':[0,0,0,0,0,0,0,0,0,0,0],
             'Transition':[0,0,0,0,0,0,0,0,0,0,0],
             'Knockout':[0,0,0,0,0,0,0,0,0,0,0],
             'Good Pass':[0,0,0,0,0,0,0,0,0,0,0],
             'Points':[0,0,0,0,0,0,0,0,0,0,0]
            }
  sum_df = pd.DataFrame.from_dict(df_dict)

  #print(sum_df)
  # start a loop over the weeks
  for i in range(0,num_weeks):
    week_var = 'Week '+str(i+1)
    # filter ppr_df to tmp_df for this week
    #print(f"type of ppr_df game_date: {type(ppr_df['game_date'])}")
    #print(f"weekly dates : {type(weekly_dates)}, [i] {type(weekly_dates[i])}, [i][start_date] {type(weekly_dates[i]['start_date'])}")
    #print(f"weekly dates : {weekly_dates}, [i] {weekly_dates[i]}, [i][start_date] {weekly_dates[i]['start_date']}")

    tmp_df = ppr_df
    tmp_df['game_date'] = pd.to_datetime(tmp_df['game_date'])
    tmp_df['game_date'] = tmp_df['game_date'].dt.date
    #m_ppr_df = m_ppr_df.loc[(m_ppr_df['game_date'] >= disp_start_date) & (m_ppr_df['game_date'] <= disp_end_date) ]
    #print(f"Limitiing by Dates:{disp_start_date},{disp_end_date}")

    tmp_df = tmp_df[ (tmp_df['game_date'] >= weekly_dates[i]['start_date'].date()) & (tmp_df['game_date'] < weekly_dates[i]['end_date'].date()) ]

    #print(f"week : {i}, week var: {week_var}, start date: {weekly_dates[i]['start_date'].date()}, end date: {weekly_dates[i]['end_date'].date()}, number of points: {tmp_df.shape[0]}")

    pt_totals_df = pair_pt_total( tmp_df, disp_pair )
    sum_df.loc[i,'FBHE'] = (pt_totals_df.at[0,'p_fbk']-pt_totals_df.at[0,'p_fbe'])/( pt_totals_df.at[0,'p_att_total'])
    sum_df.loc[i,'Errors' ] = (pt_totals_df.at[0,'p_fbe']+pt_totals_df.at[0,'p_tse']+pt_totals_df.at[0,'p_te_r']+pt_totals_df.at[0,'p_te_s'])/( pt_totals_df.at[0,'pts_total'])
    sum_df.loc[i,'Transition' ] = (pt_totals_df.at[0,'p_tk_s']+pt_totals_df.at[0,'p_tk_r']+pt_totals_df.at[0,'o_te_r']+pt_totals_df.at[0,'o_te_s'])/( pt_totals_df.at[0,'trans_total'])
    sum_df.loc[i,'Knockout'] = (pt_totals_df.at[0,'p_tsa']+pt_totals_df.at[0,'o_bad_pass'])/( pt_totals_df.at[0,'p_serves'])
    sum_df.loc[i,'Good Pass'] = (pt_totals_df.at[0,'p_good_pass'])/( pt_totals_df.at[0,'p_good_pass']+pt_totals_df.at[0,'p_bad_pass'])
    sum_df.loc[i,'Points' ] = ( (pt_totals_df.at[0,'p_tsa']+pt_totals_df.at[0,'p_fbk']+pt_totals_df.at[0,'p_tk_r']+pt_totals_df.at[0,'p_tk_s']) +
                                (pt_totals_df.at[0,'o_tse']+pt_totals_df.at[0,'o_fbe']+pt_totals_df.at[0,'o_te_r']+pt_totals_df.at[0,'o_te_s']) ) / ( pt_totals_df.at[0,'pts_total']
                                                                                                                                                    )
    # format the entries
    sum_df.loc[i,'FBHE'] = "{:.3f}".format(sum_df.loc[i,'FBHE'])
    sum_df.loc[i,'Errors'] = "{:.3f}".format(sum_df.loc[i,'Errors'])
    sum_df.loc[i,'Transition'] = "{:.3f}".format(sum_df.loc[i,'Transition'])
    sum_df.loc[i,'Knockout'] = "{:.3f}".format(sum_df.loc[i,'Knockout'])
    sum_df.loc[i,'Good Pass'] = "{:.3f}".format(sum_df.loc[i,'Good Pass'])
    sum_df.loc[i,'Points'] = "{:.3f}".format(sum_df.loc[i,'Points'])

  #print(f" Summary dataframe: \n {sum_df}")

  
  # now create histograms for each one
  size = [11,5]
  avg_title = disp_league + " Average"
  plt1 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['FBHE'].tolist(), 'First Ball Hitting Efficiency', '', 'FBHE', size, avg_title, pair_data_stats_df.at[0,'fbhe_mean'],False,0,0  )
  plt2 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Errors'].tolist(), 'Error Denisty', '', 'Error Denisty', size, avg_title, pair_data_stats_df.at[0,'err_den_mean']/50,False,0,0 )
  # error density that is stored is a percentage for the player, so time 100 to get to a deciimal, then tiimes two to match for a pair.
  plt3 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Transition'].tolist(), 'Transition Conversion', '', 'Transition Conversion', size, avg_title, pair_data_stats_df.at[0,'tcr_mean']/100,False,0, 0 )
  # tcr is stored as a percent, so divide by 100 to match the calculations here
  plt4 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Knockout'].tolist(), 'Serving Aggressiveness', '', 'Serving - Knockout Percent', size, avg_title, pair_data_stats_df.at[0,'knockout_mean'],False,0 ,0 )
  plt5 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Good Pass'].tolist(), 'Passing Quality', '', 'Percent Good Passes', size, avg_title, pair_data_stats_df.at[0,'goodpass_mean'],False,0,0 )
  plt6 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Points'].tolist(), 'Percent of Points Won', '', 'Percent of Points Earned', size, '', 0,False,0,0 )

  # store the images in the list
  image_list[0] = plt1
  image_list[1] = plt2
  image_list[2] = plt3
  image_list[3] = plt4
  image_list[4] = plt5
  image_list[5] = plt6

  return title_list, label_list, image_list, df_list

#---------------------------------------------------------------------------------------------------
#
#.  Leagye Report(s)
#
#-------------------------------------------------------------------------------------------------
@anvil.server.callable
def report_league_new(lgy, team, **rpt_filters):
  '''
  Report Functions:

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  '''
  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  title_list, label_list, image_list, df_list = initialize_report_lists(inspect.currentframe().f_code.co_name, **rpt_filters)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  pair_data_df, pair_data_stats_df = get_pair_data(disp_league, disp_gender, disp_year)
  tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  #------------------------------------------------------------------------------------------------------
  #            Set to a Player, Pair, or Team Report
  #------------------------------------------------------------------------------------------------------
  disp_player = rpt_filters.get('player')
  disp_pair = rpt_filters.get('pair')
  # for a player report:
  if False:  # set only one of these to True
    ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
      (ppr_df['player_a2'] == disp_player) |
      (ppr_df['player_b1'] == disp_player) |
      (ppr_df['player_b2'] == disp_player) 
      ]
  # for a pair report:
  if False:  # set only one of these to True
    ppr_df = ppr_df[ (ppr_df['teama'] == disp_pair) | (ppr_df['teamb'] == disp_pair) ]

  # for a team report
  if False:
    ppr_df = ppr_df[ ppr_df['teama'] == disp_pair ]

  #------------------------------------------------------------------------------------------------------
  #           Define the suymmary / correlation dataframe
  #------------------------------------------------------------------------------------------------------

  # Create the output dictionary
  df_dict = {
    'Metric': ['League', 'Points', 'Sets', 'Players', 'Pairs', 'Win w/ Hgher FBHE', 'Win w/ Higher Transition', 'Win w/ Lower Errors',' ', 'High FBHE Loser w/High Errors','High FBHE Loser w/Low Transition'],
    'Number': ['', 0, 0, 0, 0, 0, 0, 0, '', 0, 0],
    'Percent': ['', '', '', '', '', 0, 0, 0, '', 0, 0]
  }

  # Create DataFrame and set index
  df_table = pd.DataFrame.from_dict(df_dict).set_index('Metric')

  # count points as the number of rows in ppr:
  df_table.at['League','Number'] = lgy
  df_table.at['Points','Number'] = ppr_df.shape[0]
  df_table.at['Sets','Number'] = tri_df.shape[0]
  df_table.at['Players','Number'] = player_data_df.shape[0]
  df_table.at['Pairs','Number'] = pair_data_df.shape[0]
  
  df_table.at['Win w/ Hgher FBHE','Number'] = tri_df[ (tri_df['win_fbhe_withace'] >= tri_df['loser_fbhe_withace'])  ].shape[0]
  df_table.at['Win w/ Hgher FBHE','Percent'] = str('{:.1%}'.format(df_table.at['Win w/ Hgher FBHE','Number']/df_table.at['Sets','Number']))
  
  df_table.at['Win w/ Lower Errors','Number'] = tri_df[ (tri_df['win_err_den'] < tri_df['loser_err_den']) ].shape[0]
  df_table.at['Win w/ Lower Errors','Percent'] = str('{:.1%}'.format(df_table.at['Win w/ Lower Errors','Number']/df_table.at['Sets','Number']))
  
  df_table.at['Win w/ Higher Transition','Number'] = tri_df[ (tri_df['win_tcr'] > tri_df['loser_tcr']) ].shape[0]
  df_table.at['Win w/ Higher Transition','Percent'] = str('{:.1%}'.format(df_table.at['Win w/ Higher Transition','Number']/df_table.at['Sets','Number']))

  # Extract scalar thresholds
  tcr_low = (player_data_stats_df['tcr_mean'].iloc[0] + player_data_stats_df['tcr_stdev'].iloc[0])/100
  error_high = (player_data_stats_df['err_den_mean'].iloc[0] + player_data_stats_df['err_den_stdev'].iloc[0])/100

  # Filter tri_df
  tmp_df = tri_df[tri_df['win_fbhe_withace'] < tri_df['loser_fbhe_withace'] ]

  #df_table.at['Low FBHE Win w/Low Transition','Number'] = tmp_df[ (tmp_df['win_tcr'] < tcr_low) ].shape[0] 
  df_table.at['High FBHE Loser w/High Errors','Number'] = tmp_df[( tmp_df['loser_err_den'] >= error_high ) ].shape[0]
  df_table.at['High FBHE Loser w/Low Transition','Number'] = tmp_df[( tmp_df['loser_tcr'] <= tcr_low ) ].shape[0]
  
  if tmp_df.shape[0] == 0:
    #df_table.at['Low FBHE Win w/Low Transition','Percent'] = 0
    df_table.at['High FBHE Loser w/High Errors','Percent'] = 0
    df_table.at['High FBHE Loser w/Low Transition','Percent'] = 0
  else:
    #df_table.at['Low FBHE Win w/Low Transition','Percent'] = str('{:.1%}'.format(df_table.at['Low FBHE Win w/Low Transition','Number']/tmp_df.shape[0]))
    df_table.at['High FBHE Loser w/High Errors','Percent'] = str('{:.1%}'.format(df_table.at['High FBHE Loser w/High Errors','Number']/tmp_df.shape[0]))
    df_table.at['High FBHE Loser w/Low Transition','Percent'] = str('{:.1%}'.format(df_table.at['High FBHE Loser w/Low Transition','Number']/tmp_df.shape[0]))
  
  #print(f'reports league: df_table \n {df_table}')
  # put the DF's in the df_list
  df_table = df_table.reset_index()
  df_list[0] = df_table.to_dict('records')


  #------------------------------------------------------------------------------------------------------
  #          Define Images and dataframes for 5 plots
  #------------------------------------------------------------------------------------------------------

  plt_size = [20,15]

  # Image for the Historgram of FBHE
  stat_text, hist_plot = plot_histogram( player_data_df,'fbhe','First Ball Hitting Efficiency', -1, 1)
  image_list[0] = hist_plot
  #print(f" stat_text: {stat_text}")
  df = pd.DataFrame({'':[stat_text]})
  df_list[1] = df.to_dict('records')
  
  # Image for the Historgram of Error Density
  stat_text, hist_plot = plot_histogram( player_data_df,'err_den','Error Density', 0, 100)
  image_list[2] = hist_plot
  #print(f" stat_text: {stat_text}")
  df = pd.DataFrame({'':[stat_text]})
  df_list[2] = df.to_dict('records')
  
  # Image for the Historgram of Transition Conversion
  stat_text, hist_plot = plot_histogram( player_data_df,'tcr','Transition Conversion', 0, 100)
  image_list[4] = hist_plot
  #print(f" stat_text: {stat_text}")
  df = pd.DataFrame({'':[stat_text]})
  df_list[3] = df.to_dict('records')
  
  # Image for the Historgram of Knock Out
  stat_text, hist_plot = plot_histogram( pair_data_df,'knockout','Serving Aggressiveness - Knock Out', 0,1)
  image_list[6] = hist_plot
  #print(f" stat_text: {stat_text}")
  df = pd.DataFrame({'':[stat_text]})
  df_list[4] = df.to_dict('records')
  
  # Image for the Historgram of Good Pass Percent
  stat_text, hist_plot = plot_histogram( player_data_df,'goodpass','Percent Good Passes', 0.1, 1)
  image_list[8] = hist_plot
  #print(f" stat_text: {stat_text}")
  df = pd.DataFrame({'':[stat_text]})
  df_list[5] = df.to_dict('records')

  # Image for the Bar Graph of FBHE vs winning %
  high = player_data_stats_df.at[0,'fbhe_mean']+2*player_data_stats_df.at[0,'fbhe_stdev']
  low = player_data_stats_df.at[0,'fbhe_mean']-2*player_data_stats_df.at[0,'fbhe_stdev']
  bar_width = (high-low)/30
  plot_df, status = count_wins( tri_df['win_fbhe_noace'], tri_df['loser_fbhe_noace'], low, high )
  bar_plot = plot_bar_graph( plot_df['MidPoint'], plot_df['Win Percent'], 'Winning Percent by FBHE', 'FBHE', 'Percent Wins', plt_size, '', 0, True, player_data_stats_df.at[0,'fbhe_mean'], player_data_stats_df.at[0,'fbhe_stdev'], bar_width)
  image_list[1] = bar_plot

  # Image for the Bar Graph of Error Denisty vs winning %
  high = (player_data_stats_df.at[0,'err_den_mean']+2*player_data_stats_df.at[0,'err_den_stdev'])/100
  low = (player_data_stats_df.at[0,'err_den_mean']-2*player_data_stats_df.at[0,'err_den_stdev'])/100
  bar_width = (high-low)/30
  plot_df, status = count_wins( tri_df['win_err_den'], tri_df['loser_err_den'], low, high )
  bar_plot = plot_bar_graph( plot_df['MidPoint'], plot_df['Win Percent'], 'Winning Percent by Error Density', 'Error Density', 'Percent Wins', plt_size, '', 0, True, player_data_stats_df.at[0,'err_den_mean']/100, player_data_stats_df.at[0,'err_den_stdev']/100, bar_width)
  image_list[3] = bar_plot

  # Image for the Bar Graph of TCR vs winning %
  high = (player_data_stats_df.at[0,'tcr_mean']+2*player_data_stats_df.at[0,'tcr_stdev'])/100
  low = (player_data_stats_df.at[0,'tcr_mean']-2*player_data_stats_df.at[0,'tcr_stdev'])/100
  bar_width = (high-low)/30
  plot_df, status = count_wins( tri_df['win_tcr'], tri_df['loser_tcr'], low, high )
  bar_plot = plot_bar_graph( plot_df['MidPoint'], plot_df['Win Percent'], 'Winning Percent by Transition Conversion Rate', 'Transition Conversion Rate', 'Percent Wins', plt_size, '', 0, True, player_data_stats_df.at[0,'tcr_mean']/100, player_data_stats_df.at[0,'tcr_stdev']/100, bar_width)
  image_list[5] = bar_plot

  # Image for the Bar Graph of Knock Out vs winning %
  high = player_data_stats_df.at[0,'knockout_mean']+2*player_data_stats_df.at[0,'knockout_stdev']
  low = player_data_stats_df.at[0,'knockout_mean']-2*player_data_stats_df.at[0,'knockout_stdev']
  bar_width = (high-low)/30
  plot_df, status = count_wins( tri_df['win_knockout'], tri_df['loser_knockout'], low, high )
  bar_plot = plot_bar_graph( plot_df['MidPoint'], plot_df['Win Percent'], 'Winning Percent by Knockoout Rate', 'Knockout', 'Percent Wins', plt_size, '', 0, True, player_data_stats_df.at[0,'knockout_mean'], player_data_stats_df.at[0,'knockout_stdev'], bar_width)
  image_list[7] = bar_plot

  # Image for the Bar Graph of Good Pass % vs winning %
  high = player_data_stats_df.at[0,'goodpass_mean']+2*player_data_stats_df.at[0,'goodpass_stdev']
  low = player_data_stats_df.at[0,'goodpass_mean']-2*player_data_stats_df.at[0,'goodpass_stdev']
  bar_width = (high-low)/30
  plot_df, status = count_wins( tri_df['win_goodpass'], tri_df['loser_goodpass'], low, high )
  bar_plot = plot_bar_graph( plot_df['MidPoint'], plot_df['Win Percent'], 'Winning Percent by Good Passes', 'Percent Good Passes', 'Percent Wins', plt_size, '', 0, True, player_data_stats_df.at[0,'goodpass_mean'], player_data_stats_df.at[0,'goodpass_stdev'], bar_width)
  image_list[9] = bar_plot
  
  #print(f"Return Values: Title List \n {title_list}")
  #print(f"Return Values: Label List \n {label_list}")
  #print(f"Return Values: Image List \n {image_list}")
  #print(f"Return Values: DF List \n {df_list}")

  return title_list, label_list, image_list, df_list


def count_wins( win_column, Loser_column, l_min, l_max):
  '''
  Take a tri_data column for the winner and the loser and return a single dataframe with mid-points and percent winners, percent losers, number of winners, number of losers

  This could be fbhe, goodpass, err_den, whatever

  l_min is logical miniumum
  l_max is logical maximum

  Status is ??

  win_column should be a dataframe column: i.e:  tri_df['fbhe_noace']
  
  '''

  # first, let's try to put these together, add a column
  col1 = win_column.tolist()  # 
  col2 = Loser_column.tolist()  # 

  # Pair col1 with 'W'
  col1_paired = [[x, 'W'] for x in col1 if not pd.isna(x)]  # 
  # Pair col2 with 'L'
  col2_paired = [[x, 'L'] for x in col2 if not pd.isna(x)]  # 
  # Append the two
  combined_list = col1_paired + col2_paired  # 

  # back to a dataframe
  # Convert to DataFrame for easier max/min
  combined_df = pd.DataFrame(combined_list, columns=['Value', 'Status'])

  # Max and min of 'Value' column
  max_value = combined_df['Value'].max() 
  if max_value > l_max:
    max_value = l_max
  min_value = combined_df['Value'].min()
  if min_value < l_min:
    min_value = l_min
  bins = 20
  step = (max_value-min_value)/bins
  #print(f"max, min, and step: Min:{min_value}, Max:{max_value}, Step:{step}")

  # Create the output dictionary
  df_dict = {
    'Metric': [0],
    'Min': [0],
    'MidPoint': [0],
    'Max':[0],
    'Wins':[0],
    'Losses':[0],
    'Win Percent':[0],
    'Loss Percent':[0]
  }

  # Create DataFrame and set index
  df_table = pd.DataFrame.from_dict(df_dict).set_index('Metric')

  #print(f" df_table :\n {df_table}")
  #print(f"Combined df: \n {combined_df}")
  
  for i in range(1,bins+1):
    min = (i-1)*step + min_value
    max = min+step
    tmp_df = combined_df[ (combined_df['Value'] >= min) & (combined_df['Value'] < max )]
    df_table.loc[i] = [0,0,0,0,0,0,0]
    df_table.at[i,'Min'] = float('{:.2}'.format(min))
    df_table.at[i,'Max'] = float('{:.2}'.format(max))
    df_table.at[i,'MidPoint'] = float('{:.2}'.format((max + min)/2))
    df_table.at[i,'Wins'] = tmp_df[ ( tmp_df['Status'] == 'W') ].shape[0]
    df_table.at[i,'Losses'] = tmp_df[ ( tmp_df['Status'] == 'L') ].shape[0]
    if tmp_df.shape[0] == 0:
      df_table.at[i,'Win Percent'] = 0
      df_table.at[i,'Loss Percent'] = 0
    else:
      df_table.at[i,'Win Percent'] = float('{:.2}'.format(df_table.at[i,'Wins']/tmp_df.shape[0]))
      df_table.at[i,'Loss Percent'] = float('{:.2}'.format(df_table.at[i,'Losses']/tmp_df.shape[0]))

  #print(f' df table in count-wins functons: \n{df_table}')
  return df_table, True


#---------------------------------------------------------------------------
#
#              player strengths and weaknesses
#
#---------------------------------------------------------------------------
@anvil.server.callable
def player_sw_new(lgy, team, **rpt_filters):
  '''
  Report Functions:

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  #ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  #ppr_df = filter_ppr_df( ppr_df, **rpt_filters)

  # initiate return lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=inspect.currentframe().f_code.co_name)
  title_list[0] = rpt_row['rpt_title']
  title_list[1] = rpt_row['rpt_sub_title']
  title_list[2] = rpt_row['rpt_section_title1']
  title_list[3] = rpt_filters.get('lgy')
  title_list[4] = rpt_row['team_name']
  title_list[5] = rpt_row['rpt_type']
  title_list[6] = rpt_row['filter_text']
  title_list[7] = rpt_row['explain_text']
  title_list[8] = rpt_filters.get('player')
  title_list[9] = rpt_filters.get('pair')

  label_list[0] = rpt_row['box1_title']
  label_list[1] = rpt_row['box2_title']
  label_list[2] = rpt_row['box3_title']
  label_list[3] = rpt_row['box4_title']
  label_list[4] = rpt_row['box5_title']
  label_list[5] = rpt_row['box6_title']
  label_list[6] = rpt_row['box7_title']
  label_list[7] = rpt_row['box8_title']
  label_list[8] = rpt_row['box9_title']
  label_list[9] = rpt_row['box10_title']

  #------------------------------------------------------------------------------------------------------
  #
  #            Create the images and dataframes with filtered ppr data for report
  #
  #-----------------------------------------------------------------------------------------------------

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  #ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
  #  (ppr_df['player_a2'] == disp_player) |
  #  (ppr_df['player_b1'] == disp_player) |
  #  (ppr_df['player_b2'] == disp_player) 
  #  ]


  # break disp_player into team, number, and shortname
  # unpack player into team, number and short name
  str_loc = disp_player.index(' ')
  p_team = disp_player[:str_loc].strip()
  p_player = disp_player[str_loc+1:]
  str_loc = p_player.index(' ')
  p_num = p_player[:str_loc].strip()
  p_sname = p_player[str_loc+1:].strip()
  sw_list = [(f_row['s_w']) for f_row in app_tables.master_player.search(league=disp_league,gender=disp_gender,year=disp_year,team=p_team,number=p_num,shortname=p_sname)]

  #print(f"sw_list: {sw_list}")
  if sw_list:
    if sw_list[0]:
      # sw_list is now a media object, need to turn it back into a dataframe
      sw_df =  pd.read_csv(io.BytesIO( sw_list[0].get_bytes()))

      # now we are going split s & w into 3 
      # now calculate the Offense strength and weakness markdown
      off_df = sw_df[ sw_df['Section'] == 'Offense'] 
      if off_df.shape[0] == 0:
        off_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})
      else:
        off_df = off_df.sort_values(by='Category', ascending=True, na_position='last')
        off_df = off_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
        off_df = off_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})
      #print(f"pair_sw_report: off_df: {off_df}")

      # now calculate the Deffense strength and weakness markdown
      def_df = sw_df[ sw_df['Section'] == 'Serving']
      if def_df.shape[0] == 0:
        def_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})
      else:
        def_df = def_df.sort_values(by='Category', ascending=True, na_position='last')
        def_df = def_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
        def_df = def_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})
      #print(f"pair_sw_report: def_df: {def_df}")

      # now calculate the Errors strength and weakness markdown
      err_df = sw_df[ sw_df['Section'] == 'Error & Transition'] 
      if err_df.shape[0] == 0:
        err_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})
      else:
        err_df = err_df.sort_values(by='Category', ascending=True, na_position='last')
        err_df = err_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
        err_df = err_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})
      #print(f"pair_sw_report: err_df: {err_df}")

      # now calculate the Errors strength and weakness markdown
      cons_df = sw_df[ sw_df['Section'] == 'Consistency'] 
      if cons_df.shape[0] == 0:
        cons_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})
      else:
        cons_df = cons_df.sort_values(by='Category', ascending=True, na_position='last')
        cons_df = cons_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
        cons_df = cons_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})
      #print(f"pair_sw_report: consistenct_df: {cons_df}")

    else:
      off_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})
      def_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})
      err_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})
      cons_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})
  else:
    off_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})
    def_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})
    err_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})
    cons_df = pd.DataFrame({'Description':['No Data Found'],'Category':['No Data Found']})

  # put the DF's in the df_list
  df_list[0] = off_df.to_dict('records')
  df_list[1] = def_df.to_dict('records')
  df_list[2] = err_df.to_dict('records')
  df_list[3] = cons_df.to_dict('records')

  return title_list, label_list, image_list, df_list


#---------------------------------------------------------------------------
#
#              player FBHE by 45 Zones
#
#---------------------------------------------------------------------------
@anvil.server.callable
def player_45_fbhe_new(lgy, team, **rpt_filters):
  '''
  Report Functions:
    - Caluclate and display the 45 zonesm plus general zone 1,3,5 table
    - For this reports, calculate FBHE for each of the 45 zones

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  #
  # comment out the dataframe not needed for this report
  #
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  #print(f"PPR DF size, new {ppr_df.shape[0]}")
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #pair_data_df, pair_data_stats_df = get_pair_data(disp_league, disp_gender, disp_year)
  #tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # initiate return lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=inspect.currentframe().f_code.co_name)
  title_list[0] = rpt_row['rpt_title']
  title_list[1] = rpt_row['rpt_sub_title']
  title_list[2] = rpt_row['rpt_section_title1']
  title_list[3] = rpt_filters.get('lgy')
  title_list[4] = rpt_row['team_name']
  title_list[5] = rpt_row['rpt_type']
  title_list[6] = rpt_row['filter_text']
  title_list[7] = rpt_row['explain_text']
  title_list[8] = rpt_filters.get('player')
  title_list[9] = rpt_filters.get('pair')

  label_list[0] = rpt_row['box1_title']
  label_list[1] = rpt_row['box2_title']
  label_list[2] = rpt_row['box3_title']
  label_list[3] = rpt_row['box4_title']
  label_list[4] = rpt_row['box5_title']
  label_list[5] = rpt_row['box6_title']
  label_list[6] = rpt_row['box7_title']
  label_list[7] = rpt_row['box8_title']
  label_list[8] = rpt_row['box9_title']
  label_list[9] = rpt_row['box10_title']

  #------------------------------------------------------------------------------------------------------
  #
  #            Create the images and dataframes with filtered ppr data for report
  #
  #-----------------------------------------------------------------------------------------------------

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]
  #print(f"ppr size after filter for the player: {disp_player}, {ppr_df.shape[0]}")

  # break disp_player into team, number, and shortname
  # unpack player into team, number and short name
  str_loc = disp_player.index(' ')
  p_team = disp_player[:str_loc].strip() # player team
  p_player = disp_player[str_loc+1:]
  str_loc = p_player.index(' ')
  p_num = p_player[:str_loc].strip() # player number
  p_sname = p_player[str_loc+1:].strip() # player short name


  #------------------------------------------------------------------------------------
  #
  #     Report is 'set up', not calcualte acorss the 3 zones, then the 45 serves
  #
  #-------------------------------------------------------------------------------------
  
  # create the output dataframe - This dataframe is the summary for zone 1,3,5
  df_dict = {' ':['FBHE','FBSO','Kills','Errors','Attempts','URL'],
             'All':[0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,' '],
             'No Zone':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( ppr_df, disp_player, 'pass', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[2,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[3,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[4,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[1,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 3','Zone 5','No Zone']
    for i in [0,1,2,3]:
      zone = 0 if i == 3 else (i*2)+1
      fbhe_vector = fbhe( ppr_df[ppr_df['serve_src_zone_net']==zone], disp_player, 'pass', True )
      fbhe_table.at[0,column[i]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[2,column[i]] = fbhe_vector[1]  # attacks
      fbhe_table.at[3,column[i]] = fbhe_vector[2]  # errors
      fbhe_table.at[4,column[i]] = fbhe_vector[3]  # attempts
      fbhe_table.at[1,column[i]] = fbhe_vector[4]  # fbso
      fbhe_table.at[5,column[i]] = fbhe_vector[5]  # URL


  else:
    fbhe_table.at[0,'All'] = "No Data Found"

    # get high and low for the color scheme, mean +/- 2 sdtd
  cmax = player_data_stats_df.at[0,'fbhe_mean']+2*player_data_stats_df.at[0,'fbhe_stdev']
  cmin = player_data_stats_df.at[0,'fbhe_mean']-2*player_data_stats_df.at[0,'fbhe_stdev']

  # the order of the index
  '''
  index.  from.   to   
  0.      1.       1C.  x = 0.8. y = 4.0
  1.      1.       1D.  x = 0.8. y = 5.6
  2.       1.      1E.   x = 0.8 y = 7.2
  3       1.      2C.    x = 2.4. y = 4.0
  4       1       2D.    x = 2.4  y = 5.6
  5.      1.      2E.    x = 2.4. y = 7.2
  6                3C.   x = 3.6. y = 4.0
  7                3D.    x = 3.6. y = 5.6
  8                3E.   x = 3.6. y = 7.2
  9                4C
  10               4D
  '''
  # now, get the variables 
  pass_x = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass_y = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass1_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass3_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass5_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]

  # create the output dataframe - This dataframe is the for a detail below each graph, 1,3,5, so we can see the fnhe #, and the URL
  df_dict = {'Dest Zone':[' '],
             'FBHE':[0],
             'Att':[0],
             'URL':[' ']
            }
  z1_table = pd.DataFrame.from_dict( df_dict )
  z3_table = pd.DataFrame.from_dict( df_dict )
  z5_table = pd.DataFrame.from_dict( df_dict )
  z1_table_index = 0
  z3_table_index = 0
  z5_table_index = 0

  # now, loop thru the list for serves from zone 1
  index = 0
  x = 8.8
  for i in [1,2,3,4,5]:  # j is along the net
    x = x - 1.6
    y = 2.4
    for j in ['c','d','e']: # k is depth+
      y = y + 1.6
      pass_x[index] = x
      pass_y[index] = y

      # Now let's calcualte from PPR data:
      #print(f"size of ppr_df: {ppr_df.shape[0]}")
      #print(f"size of ppr_df, srv src = 1: {ppr_df[ppr_df['serve_src_zone_net'] == 1].shape[0]}")
      #print(f"size of ppr_df, srv src = 1, dest = {i}: {ppr_df[(ppr_df['serve_src_zone_net'] == 1)&(ppr_df['serve_dest_zone_net'] == i)].shape[0]}")
      #print(f"size of ppr_df, srv src = 1, dest = {i}{j}: {ppr_df[(ppr_df['serve_src_zone_net'] == 1)&(ppr_df['serve_dest_zone_net'] == i)&(ppr_df['serve_dest_zone_depth'] == j.capitalize() )].shape[0]}")

      # Zone 1
      fbhe_vector = fbhe(ppr_df[  (ppr_df['serve_src_zone_net'] == 1) &
                                  (ppr_df['serve_dest_zone_net'] == i) &
                                  (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                         disp_player, 'pass', True
        )
      #print(f"FBHE vector for 1, {i}{j}, {fbhe_vector}")
      if fbhe_vector[3] >= 5:
        pass1_val[index] = fbhe_vector[0]
        z1_table.loc[z1_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z1_table.loc[z1_table_index,'FBHE'] = fbhe_vector[0]
        z1_table.loc[z1_table_index,'Att'] = fbhe_vector[3]
        z1_table.loc[z1_table_index,'URL'] = fbhe_vector[5]
        z1_table_index = z1_table_index + 1
        

      # Zone 3
      fbhe_vector = fbhe(ppr_df[  (ppr_df['serve_src_zone_net'] == 3) &
                                  (ppr_df['serve_dest_zone_net'] == i) &
                                  (ppr_df['serve_dest_zone_depth'] == j.capitalize())],
                         disp_player, 'pass', True
                        )
      if fbhe_vector[3] >= 5:        
        pass3_val[index] = fbhe_vector[0]
        z3_table.loc[z3_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z3_table.loc[z3_table_index,'FBHE'] = fbhe_vector[0]
        z3_table.loc[z3_table_index,'Att'] = fbhe_vector[3]
        z3_table.loc[z3_table_index,'URL'] = fbhe_vector[5]
        z3_table_index = z3_table_index + 1

      # Zone 5
      fbhe_vector = fbhe(ppr_df[  (ppr_df['serve_src_zone_net'] == 5) &
                                  (ppr_df['serve_dest_zone_net'] == i) &
                                  (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                         disp_player, 'pass', True
                        )
      if fbhe_vector[3] >= 5:      
        pass5_val[index] = fbhe_vector[0]
        z5_table.loc[z5_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z5_table.loc[z5_table_index,'FBHE'] = fbhe_vector[0]
        z5_table.loc[z5_table_index,'Att'] = fbhe_vector[3]
        z5_table.loc[z5_table_index,'URL'] = fbhe_vector[5]
        z5_table_index = z5_table_index + 1

      index = index + 1

  # I should now have the tables required
  #print(f"x,y : {pass_x}, {pass_y}")
  #print(f"pass value 1:\n {pass1_val}, Pass Value 3:\n{pass3_val},  Pass Value 3:\n{pass5_val}")

  # make x,y for serve lines:
  x11 = [0.5,0.5,0.5]
  x12 = [0,4,8]
  x31 = [4,4,4]
  x51 = [7.5,7.5,7.5]
  y1 = [-8,-8,-8]
  y2 = [8,8,8]

  # Create the plot for serves from Zone 1 - define the figure, plot the court, plot a few serve lines, plot the dots
  #cm = mpl.cm.cool
  #norm = mpl.colors.Normalize(vmin=-1, vmax=1)

  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x11, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass1_val, vmin=cmin, vmax=cmax, cmap='PiYG' )  
  z1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass3_val, vmin=cmin, vmax=cmax, cmap='PiYG' ) 
  z3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass5_val, vmin=cmin, vmax=cmax, cmap='PiYG' )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap='PiYG'),ax=ax, orientation='vertical', label='First Ball Hitting Efficiency')
  z5_plt = anvil.mpl_util.plot_image()

  # put the Images in the image_list
  image_list[0] = z1_plt
  image_list[1] = z3_plt
  image_list[2] = z5_plt
  
  # put the DF's in the df_list
  df_list[0] = fbhe_table.to_dict('records')
  df_list[1] = z1_table.to_dict('records')
  df_list[2] = z3_table.to_dict('records')
  df_list[3] = z5_table.to_dict('records')

  return title_list, label_list, image_list, df_list


#---------------------------------------------------------------------------
#
#              player Out of System by 45 Zones
#
#---------------------------------------------------------------------------
@anvil.server.callable
def player_45_oos_new(lgy, team, **rpt_filters):
  '''
  Report Functions:
    - Caluclate and display the 45 zonesm plus general zone 1,3,5 table
    - For this reports, calculate FBHE for each of the 45 zones

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  #
  # comment out the dataframe not needed for this report
  #
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  #print(f"PPR DF size, new {ppr_df.shape[0]}")
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #pair_data_df, pair_data_stats_df = get_pair_data(disp_league, disp_gender, disp_year)
  #tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # initiate return lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=inspect.currentframe().f_code.co_name)
  title_list[0] = rpt_row['rpt_title']
  title_list[1] = rpt_row['rpt_sub_title']
  title_list[2] = rpt_row['rpt_section_title1']
  title_list[3] = rpt_filters.get('lgy')
  title_list[4] = rpt_row['team_name']
  title_list[5] = rpt_row['rpt_type']
  title_list[6] = rpt_row['filter_text']
  title_list[7] = rpt_row['explain_text']
  title_list[8] = rpt_filters.get('player')
  title_list[9] = rpt_filters.get('pair')

  label_list[0] = rpt_row['box1_title']
  label_list[1] = rpt_row['box2_title']
  label_list[2] = rpt_row['box3_title']
  label_list[3] = rpt_row['box4_title']
  label_list[4] = rpt_row['box5_title']
  label_list[5] = rpt_row['box6_title']
  label_list[6] = rpt_row['box7_title']
  label_list[7] = rpt_row['box8_title']
  label_list[8] = rpt_row['box9_title']
  label_list[9] = rpt_row['box10_title']

  #------------------------------------------------------------------------------------------------------
  #
  #            Create the images and dataframes with filtered ppr data for report
  #
  #-----------------------------------------------------------------------------------------------------

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]
  #print(f"ppr size after filter for the player: {disp_player}, {ppr_df.shape[0]}")

  # break disp_player into team, number, and shortname
  # unpack player into team, number and short name
  str_loc = disp_player.index(' ')
  p_team = disp_player[:str_loc].strip() # player team
  p_player = disp_player[str_loc+1:]
  str_loc = p_player.index(' ')
  p_num = p_player[:str_loc].strip() # player number
  p_sname = p_player[str_loc+1:].strip() # player short name


  #------------------------------------------------------------------------------------
  #
  #     Report is 'set up', noW calculate acorss the 3 zones, then the 45 serves
  #
  #-------------------------------------------------------------------------------------

  # create the output dataframe - This dataframe is the summary for zone 1,3,5
  df_dict = {' Out of System':['Number','Percent','Attempts','URL'],
             'All':[0,0,0,' '],
             'Zone 1':[0,0,0,' '],
             'Zone 3':[0,0,0,' '],
             'Zone 5':[0,0,0,' '],
             'No Zone':[0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  # oos_vector = count_out_of_system(ppr_df,disp_player,action)
  # action is 'pass', 'att', 'srv'
  # returns a vector : oos_vector[0] = number OOS (int), oos_vector[1] = percent out of system (Float()), oos_vector[2] = attempts (int())
  
  ############### Third Populate the dataframe, assuming we have data returned
  if ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = count_out_of_system( ppr_df, disp_player, 'pass' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = str('{:.1%}').format(fbhe_vector[1])  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    #fbhe_table.at[3,'All'] = fbhe_vector[3]  # URL (someday?)

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 3','Zone 5','No Zone']
    for i in [0,1,2,3]:
      zone = 0 if i == 3 else (i*2)+1
      fbhe_vector = count_out_of_system( ppr_df[ppr_df['serve_src_zone_net']==zone], disp_player, 'pass')
      fbhe_table.at[0,column[i]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i]] = str('{:.1%}').format(fbhe_vector[1])  # attacks
      fbhe_table.at[2,column[i]] = fbhe_vector[2]  # errors
      #fbhe_table.at[3,column[i]] = fbhe_vector[3]  # URL someday



  else:
    fbhe_table.at[0,'All'] = "No Data Found"

  
  # the order of the index
  '''
  index.  from.   to   
  0.      1.       1C.  x = 0.8. y = 4.0
  1.      1.       1D.  x = 0.8. y = 5.6
  2.       1.      1E.   x = 0.8 y = 7.2
  3       1.      2C.    x = 2.4. y = 4.0
  4       1       2D.    x = 2.4  y = 5.6
  5.      1.      2E.    x = 2.4. y = 7.2
  6                3C.   x = 3.6. y = 4.0
  7                3D.    x = 3.6. y = 5.6
  8                3E.   x = 3.6. y = 7.2
  9                4C
  10               4D
  '''
  # now, get the variables 
  pass_x = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass_y = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass1_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass3_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass5_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]

  # create the output dataframe - This dataframe is the for a detail below each graph, 1,3,5, so we can see the fnhe #, and the URL
  df_dict = {'Dest Zone':[' '],
             'Out Sys':[0],
             'Att':[0],
             'URL':[' ']
            }
  z1_table = pd.DataFrame.from_dict( df_dict )
  z3_table = pd.DataFrame.from_dict( df_dict )
  z5_table = pd.DataFrame.from_dict( df_dict )
  z1_table_index = 0
  z3_table_index = 0
  z5_table_index = 0

  # now, loop thru the list for serves from zone 1
  index = 0
  x = 8.8
  for i in [1,2,3,4,5]:  # j is along the net
    x = x - 1.6
    y = 2.4
    for j in ['c','d','e']: # k is depth+
      y = y + 1.6
      pass_x[index] = x
      pass_y[index] = y

      # Now let's calcualte from PPR data:
      #print(f"size of ppr_df: {ppr_df.shape[0]}")
      #print(f"size of ppr_df, srv src = 1: {ppr_df[ppr_df['serve_src_zone_net'] == 1].shape[0]}")
      #print(f"size of ppr_df, srv src = 1, dest = {i}: {ppr_df[(ppr_df['serve_src_zone_net'] == 1)&(ppr_df['serve_dest_zone_net'] == i)].shape[0]}")
      #print(f"size of ppr_df, srv src = 1, dest = {i}{j}: {ppr_df[(ppr_df['serve_src_zone_net'] == 1)&(ppr_df['serve_dest_zone_net'] == i)&(ppr_df['serve_dest_zone_depth'] == j.capitalize() )].shape[0]}")

      # Zone 1
      fbhe_vector = count_out_of_system(ppr_df[  (ppr_df['serve_src_zone_net'] == 1) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                         disp_player, 'pass'
                        )
      #print(f"OOS vector for 1, {i}{j}, {fbhe_vector}")
      if fbhe_vector[2] >= 5:
        pass1_val[index] = fbhe_vector[1]
        z1_table.loc[z1_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z1_table.loc[z1_table_index,'Out Sys'] = str('{:.1%}').format(fbhe_vector[1])
        z1_table.loc[z1_table_index,'Att'] = fbhe_vector[2]
        z1_table.loc[z1_table_index,'URL'] = ' '
        z1_table_index = z1_table_index + 1


      # Zone 3
      fbhe_vector = count_out_of_system(ppr_df[  (ppr_df['serve_src_zone_net'] == 3) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize())],
                         disp_player, 'pass'
                        )
      if fbhe_vector[2] >= 5:        
        pass3_val[index] = fbhe_vector[1]
        z3_table.loc[z3_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z3_table.loc[z3_table_index,'Out Sys'] = str('{:.1%}').format(fbhe_vector[1])
        z3_table.loc[z3_table_index,'Att'] = fbhe_vector[2]
        z3_table.loc[z3_table_index,'URL'] = ' '
        z3_table_index = z3_table_index + 1

      # Zone 5
      fbhe_vector = count_out_of_system(ppr_df[  (ppr_df['serve_src_zone_net'] == 5) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                         disp_player, 'pass'
                        )
      if fbhe_vector[2] >= 5:      
        pass5_val[index] = fbhe_vector[1]
        z5_table.loc[z5_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z5_table.loc[z5_table_index,'Out Sys'] = str('{:.1%}').format(fbhe_vector[1])
        z5_table.loc[z5_table_index,'Att'] = fbhe_vector[2]
        z5_table.loc[z5_table_index,'URL'] = ' '
        z5_table_index = z5_table_index + 1

      index = index + 1

  # I should now have the tables required
  #print(f"x,y : {pass_x}, {pass_y}")
  #print(f"pass value 1:\n {pass1_val}, Pass Value 3:\n{pass3_val},  Pass Value 3:\n{pass5_val}")

  # make x,y for serve lines:
  x11 = [0.5,0.5,0.5]
  x12 = [0,4,8]
  x31 = [4,4,4]
  x51 = [7.5,7.5,7.5]
  y1 = [-8,-8,-8]
  y2 = [8,8,8]

  # Create the plot for serves from Zone 1 - define the figure, plot the court, plot a few serve lines, plot the dots
  #cm = mpl.cm.cool
  #norm = mpl.colors.Normalize(vmin=-1, vmax=1)

  colors = ['green', 'yellow', 'red']  # Min to max
  custom_cmap = LinearSegmentedColormap.from_list('custom_red_green', colors)

  # get high and low for the color scheme, mean +/- 2 sdtd
  cmin = 1 - (player_data_stats_df.at[0,'goodpass_mean']+2*player_data_stats_df.at[0,'goodpass_stdev'])
  if cmin < 0:
    cmin = 0
  cmax = 1 - (player_data_stats_df.at[0,'goodpass_mean']-2*player_data_stats_df.at[0,'goodpass_stdev'])
  if cmax > 0.5:
    cmax = 0.5

  #print(f" cmin {cmin}, cmax {cmax}")

  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x11, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass1_val, vmin=cmin, vmax=cmax, cmap=custom_cmap )  
  z1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass3_val, vmin=cmin, vmax=cmax, cmap=custom_cmap ) 
  z3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass5_val, vmin=cmin, vmax=cmax, cmap=custom_cmap )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap=custom_cmap),ax=ax, orientation='vertical', label='First Ball Hitting Efficiency')
  z5_plt = anvil.mpl_util.plot_image()

  # put the Images in the image_list
  image_list[0] = z1_plt
  image_list[1] = z3_plt
  image_list[2] = z5_plt

  # put the DF's in the df_list
  df_list[0] = fbhe_table.to_dict('records')
  df_list[1] = z1_table.to_dict('records')
  df_list[2] = z3_table.to_dict('records')
  df_list[3] = z5_table.to_dict('records')

  return title_list, label_list, image_list, df_list



#---------------------------------------------------------------------------
#
#              player pass cluster, provide a heat map of hte location of the player's passes, baed on outcome, kill or error
#
#---------------------------------------------------------------------------
@anvil.server.callable
def player_pass_cluster_new(lgy, team, **rpt_filters):
  '''
  Report Functions:
    - Caluclate and display the 45 zonesm plus general zone 1,3,5 table
    - For this reports, calculate FBHE for each of the 45 zones

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  #
  # comment out the dataframe not needed for this report
  #
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  #print(f"PPR DF size, new {ppr_df.shape[0]}")
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  #player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #pair_data_df, pair_data_stats_df = get_pair_data(disp_league, disp_gender, disp_year)
  #tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # initiate return lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=inspect.currentframe().f_code.co_name)
  title_list[0] = rpt_row['rpt_title']
  title_list[1] = rpt_row['rpt_sub_title']
  title_list[2] = rpt_row['rpt_section_title1']
  title_list[3] = rpt_filters.get('lgy')
  title_list[4] = rpt_row['team_name']
  title_list[5] = rpt_row['rpt_type']
  title_list[6] = rpt_row['filter_text']
  title_list[7] = rpt_row['explain_text']
  title_list[8] = rpt_filters.get('player')
  title_list[9] = rpt_filters.get('pair')

  label_list[0] = rpt_row['box1_title']
  label_list[1] = rpt_row['box2_title']
  label_list[2] = rpt_row['box3_title']
  label_list[3] = rpt_row['box4_title']
  label_list[4] = rpt_row['box5_title']
  label_list[5] = rpt_row['box6_title']
  label_list[6] = rpt_row['box7_title']
  label_list[7] = rpt_row['box8_title']
  label_list[8] = rpt_row['box9_title']
  label_list[9] = rpt_row['box10_title']

  #------------------------------------------------------------------------------------------------------
  #
  #            Create the images and dataframes with filtered ppr data for report
  #
  #-----------------------------------------------------------------------------------------------------

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]
  #print(f"ppr size after filter for the player: {disp_player}, {ppr_df.shape[0]}")

  # break disp_player into team, number, and shortname
  # unpack player into team, number and short name
  str_loc = disp_player.index(' ')
  p_team = disp_player[:str_loc].strip() # player team
  p_player = disp_player[str_loc+1:]
  str_loc = p_player.index(' ')
  p_num = p_player[:str_loc].strip() # player number
  p_sname = p_player[str_loc+1:].strip() # player short name


  #------------------------------------------------------------------------------------
  #
  #     Report is 'set up', noW calculate acorss the 3 zones, then the 45 serves
  #
  #-------------------------------------------------------------------------------------


  # plot all pass locations on the court
  pass_locations_plot_object = plot_points_on_the_court(ppr_df['pass_dest_x'],ppr_df['pass_dest_y'], 1, ppr_df['video_id'], ppr_df['pass_action_id'],True,ppr_df['point_outcome'])
  
  ############### Third Populate the dataframe, assuming we have data returned
  if ppr_df.shape[0] > 0:
    plot1_return = plot_pass_clusters(ppr_df, disp_player, 'FBK')
    #print(f" Return from plot 1: {plot1_return}")
    #print(f" Return from plot 1, stat text: {plot1_return.get('stat_text')}, media list: {plot1_return.get('media_list')}")
    plot2_return = plot_pass_clusters(ppr_df, disp_player, 'FBE')
    #print(f" Return from plot 2: {plot2_return}")
    #print(f" Return from plot 2, stat text: {plot2_return.get('stat_text')}, media list: {plot2_return.get('media_list')}")
  else:
    plot1_return  = { 
      'stat_text':'Error: No data piointsin PPR_DF \n',
      'plot_image':''
        }
    plot2_return  = { 
      'stat_text':'Error: No data piointsin PPR_DF \n',
      'plot_image':''
    }

  # put the Images in the image_list
  image_list[0] = pass_locations_plot_object
  image_list[1] = plot1_return.get('plot_image')
  image_list[2] = plot2_return.get('plot_image')


  # put the DF's in the df_list
  #df_list[0] = fbhe_table.to_dict('records')


  return title_list, label_list, image_list, df_list


#---------------------------------------------------------------------------
#
#              player pass cluster, provide a heat map of hte location of the player's passes, baed on outcome, kill or error
#
#---------------------------------------------------------------------------
@anvil.server.callable
def player_45_pass_area_new(lgy, team, **rpt_filters):
  '''
  Report Functions:
    - Caluclate and display the 45 zonesm plus general zone 1,3,5 table
    - For this reports, calculate FBHE for each of the 45 zones

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  #
  # comment out the dataframe not needed for this report
  #
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  #print(f"PPR DF size, new {ppr_df.shape[0]}")
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  #player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #pair_data_df, pair_data_stats_df = get_pair_data(disp_league, disp_gender, disp_year)
  #tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # initiate return lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=inspect.currentframe().f_code.co_name)
  title_list[0] = rpt_row['rpt_title']
  title_list[1] = rpt_row['rpt_sub_title']
  title_list[2] = rpt_row['rpt_section_title1']
  title_list[3] = rpt_filters.get('lgy')
  title_list[4] = rpt_row['team_name']
  title_list[5] = rpt_row['rpt_type']
  title_list[6] = rpt_row['filter_text']
  title_list[7] = rpt_row['explain_text']
  title_list[8] = rpt_filters.get('player')
  title_list[9] = rpt_filters.get('pair')

  label_list[0] = rpt_row['box1_title']
  label_list[1] = rpt_row['box2_title']
  label_list[2] = rpt_row['box3_title']
  label_list[3] = rpt_row['box4_title']
  label_list[4] = rpt_row['box5_title']
  label_list[5] = rpt_row['box6_title']
  label_list[6] = rpt_row['box7_title']
  label_list[7] = rpt_row['box8_title']
  label_list[8] = rpt_row['box9_title']
  label_list[9] = rpt_row['box10_title']

  #------------------------------------------------------------------------------------------------------
  #
  #            Create the images and dataframes with filtered ppr data for report
  #
  #-----------------------------------------------------------------------------------------------------

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]
  #print(f"ppr size after filter for the player: {disp_player}, {ppr_df.shape[0]}")

  # break disp_player into team, number, and shortname
  # unpack player into team, number and short name
  str_loc = disp_player.index(' ')
  p_team = disp_player[:str_loc].strip() # player team
  p_player = disp_player[str_loc+1:]
  str_loc = p_player.index(' ')
  p_num = p_player[:str_loc].strip() # player number
  p_sname = p_player[str_loc+1:].strip() # player short name


  #=====================================================================================
  #-------------------------------------------------------------------------------------
  #
  #     Report is 'set up', noW calculate acorss the 3 zones, then the 45 serves
  #
  #-------------------------------------------------------------------------------------
  #=====================================================================================

  # firt, this reprot is only when the player is passing, so:
  ppr_df = ppr_df[ppr_df['pass_player'] == disp_player] 
  
  # create the output dataframe - This dataframe is the summary for zone 1,3,5
  df_dict = {' Pass Area':['Area','Attempts','URL'],
             'All':[0,0,' '],
             'Zone 1':[0,0,' '],
             'Zone 3':[0,0,' '],
             'Zone 5':[0,0,' '],
             'No Zone':[0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  # storing the area so I can get max and min for the graph
  el_area = []
  
  # oos_vector = count_out_of_system(ppr_df,disp_player,action)
  # action is 'pass', 'att', 'srv'
  # returns a vector : oos_vector[0] = number OOS (int), oos_vector[1] = percent out of system (Float()), oos_vector[2] = attempts (int())

  ############### Third Populate the dataframe, assuming we have data returned
  if ppr_df.shape[0] > 0:

    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    el_result = find_ellipse_area(ppr_df, 'pass', min_att=5)
    if el_result.get('attempts') >= 5:
      fbhe_table.at[0,'All'] = str('{:.1f}').format(el_result.get('area'))
      fbhe_table.at[1,'All'] = el_result.get('attempts')
      fbhe_table.at[2,'All'] = el_result.get('URL')  
      el_area.append(el_result.get('area'))
    else:
      fbhe_table.at[0,'All'] = ''
      fbhe_table.at[1,'All'] = ''
      fbhe_table.at[2,'All'] = '' 


    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 3','Zone 5','No Zone']
    for i in [0,1,2,3]:
      zone = 0 if i == 3 else (i*2)+1
      el_result = find_ellipse_area(ppr_df[ppr_df['serve_src_zone_net']==zone], 'pass', min_att=5)
      if el_result.get('attempts') >= 5:
        fbhe_table.at[0,column[i]] = str('{:.1f}').format(el_result.get('area'))
        fbhe_table.at[1,column[i]] = el_result.get('attempts')
        fbhe_table.at[2,column[i]] = el_result.get('URL')
        el_area.append(el_result.get('area'))
      else:
        fbhe_table.at[0,column[i]] = ' '
        fbhe_table.at[1,column[i]] = ''
        fbhe_table.at[2,column[i]] = ''
        

  else:
    fbhe_table.at[0,'All'] = "No Data Found"


  # the order of the index
  '''
  index.  from.   to   
  0.      1.       1C.  x = 0.8. y = 4.0
  1.      1.       1D.  x = 0.8. y = 5.6
  2.       1.      1E.   x = 0.8 y = 7.2
  3       1.      2C.    x = 2.4. y = 4.0
  4       1       2D.    x = 2.4  y = 5.6
  5.      1.      2E.    x = 2.4. y = 7.2
  6                3C.   x = 3.6. y = 4.0
  7                3D.    x = 3.6. y = 5.6
  8                3E.   x = 3.6. y = 7.2
  9                4C
  10               4D
  '''
  # now, get the variables 
  pass_x = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass_y = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass1_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass3_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass5_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]

  # create the output dataframe - This dataframe is the for a detail below each graph, 1,3,5, so we can see the fnhe #, and the URL
  df_dict = {'Dest Zone':[' '],
             'Area':[0],
             'Att':[0],
             'URL':[' ']
            }
  z1_table = pd.DataFrame.from_dict( df_dict )
  z3_table = pd.DataFrame.from_dict( df_dict )
  z5_table = pd.DataFrame.from_dict( df_dict )
  z1_table_index = 0
  z3_table_index = 0
  z5_table_index = 0

  # now, loop thru the list for serves from zone 1
  index = 0
  x = 8.8
  for i in [1,2,3,4,5]:  # j is along the net
    x = x - 1.6
    y = 2.4
    for j in ['c','d','e']: # k is depth+
      y = y + 1.6
      pass_x[index] = x
      pass_y[index] = y

      # Now let's calcualte from PPR data:
      #print(f"size of ppr_df: {ppr_df.shape[0]}")
      #print(f"size of ppr_df, srv src = 1: {ppr_df[ppr_df['serve_src_zone_net'] == 1].shape[0]}")
      #print(f"size of ppr_df, srv src = 1, dest = {i}: {ppr_df[(ppr_df['serve_src_zone_net'] == 1)&(ppr_df['serve_dest_zone_net'] == i)].shape[0]}")
      #print(f"size of ppr_df, srv src = 1, dest = {i}{j}: {ppr_df[(ppr_df['serve_src_zone_net'] == 1)&(ppr_df['serve_dest_zone_net'] == i)&(ppr_df['serve_dest_zone_depth'] == j.capitalize() )].shape[0]}")

      # Zone 1
      el_result = find_ellipse_area(ppr_df[  (ppr_df['serve_src_zone_net'] == 1) &
                                              (ppr_df['serve_dest_zone_net'] == i) &
                                              (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                                    'pass', min_att=5
                                   )
      #print(f"el result for zone 1: attempts: {el_result.get('attempts')}, area: {el_result.get('area')}")
      if el_result.get('attempts') >= 5:
        pass1_val[index] = el_result.get('area')
        z1_table.loc[z1_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z1_table.loc[z1_table_index,'Area'] = str('{:.1f}').format(el_result.get('area'))
        z1_table.loc[z1_table_index,'Att'] = el_result.get('attempts')
        z1_table.loc[z1_table_index,'URL'] = el_result.get('URL')
        el_area.append(el_result.get('area'))
        z1_table_index = z1_table_index + 1

      # Zone 3
      el_result = find_ellipse_area(ppr_df[  (ppr_df['serve_src_zone_net'] == 3) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                                    'pass', min_att=5
                                   )
      #print(f"el result for zone 3: attempts: {el_result.get('attempts')}, area: {el_result.get('area')}")
      if el_result.get('attempts') >= 5:
        pass3_val[index] = el_result.get('area')
        z3_table.loc[z3_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z3_table.loc[z3_table_index,'Area'] = str('{:.1f}').format(el_result.get('area'))
        z3_table.loc[z3_table_index,'Att'] = el_result.get('attempts')
        z3_table.loc[z3_table_index,'URL'] = el_result.get('URL')
        el_area.append(el_result.get('area'))
        z3_table_index = z3_table_index + 1

      # Zone 5
      el_result = find_ellipse_area(ppr_df[  (ppr_df['serve_src_zone_net'] == 5) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                                    'pass', min_att=5
                                   )
      #print(f"el result for zone 5: attempts: {el_result.get('attempts')}, area: {el_result.get('area')}")
      if el_result.get('attempts') >= 5:
        pass5_val[index] = el_result.get('area')
        z5_table.loc[z5_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z5_table.loc[z5_table_index,'Area'] = str('{:.1f}').format(el_result.get('area'))
        z5_table.loc[z5_table_index,'Att'] = el_result.get('attempts')
        z5_table.loc[z5_table_index,'URL'] = el_result.get('URL')
        el_area.append(el_result.get('area'))
        z5_table_index = z5_table_index + 1

      index = index + 1

  # I should now have the tables required
  #print(f"x,y : {pass_x}, {pass_y}")
  #print(f"pass value 1:\n {pass1_val}, Pass Value 3:\n{pass3_val},  Pass Value 3:\n{pass5_val}")

  # make x,y for serve lines:
  x11 = [0.5,0.5,0.5]
  x12 = [0,4,8]
  x31 = [4,4,4]
  x51 = [7.5,7.5,7.5]
  y1 = [-8,-8,-8]
  y2 = [8,8,8]

  # Create the plot for serves from Zone 1 - define the figure, plot the court, plot a few serve lines, plot the dots
  #cm = mpl.cm.cool
  #norm = mpl.colors.Normalize(vmin=-1, vmax=1)

  colors = ['green', 'yellow', 'red']  # Min to max
  custom_cmap = LinearSegmentedColormap.from_list('custom_red_green', colors)

  # get high and low for the color scheme, mean +/- 2 sdtd
  # i don't appear to have a max and min in the player-stats table, so ...
  cmin = min(el_area)
  if cmin < 0:
    cmin = 0
  cmax = max(el_area)
  if cmax > 20:
    cmax = 20

  #print(f" cmin {cmin}, cmax {cmax}")

  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x11, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass1_val, vmin=cmin, vmax=cmax, cmap=custom_cmap )  
  z1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass3_val, vmin=cmin, vmax=cmax, cmap=custom_cmap ) 
  z3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass5_val, vmin=cmin, vmax=cmax, cmap=custom_cmap )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap=custom_cmap),ax=ax, orientation='vertical', label='First Ball Hitting Efficiency')
  z5_plt = anvil.mpl_util.plot_image()

  # put the Images in the image_list
  image_list[0] = z1_plt
  image_list[1] = z3_plt
  image_list[2] = z5_plt

  # put the DF's in the df_list
  df_list[0] = fbhe_table.to_dict('records')
  df_list[1] = z1_table.to_dict('records')
  df_list[2] = z3_table.to_dict('records')
  df_list[3] = z5_table.to_dict('records')


  # put the DF's in the df_list
  #df_list[0] = fbhe_table.to_dict('records')


  return title_list, label_list, image_list, df_list

#------------------------------------------------------------------------------------------
#
#.   Calculate Ellipse Area
#       This is a bit differenct, calc_player_data uses similar code, but not this function (yet)
#
#--------------------------------------------------------------------------------------------------
def find_ellipse_area(tmp1_df, type, min_att=5):
  '''
  tmp1_df is the ppr dataframe with the data in one of the touch dest x,y to calculate ellipse for
  type can be:
    - 'srv'
    - 'pass'
    - 'set'
    - 'att'
    - 'dig'

    This always uses the desitnation coordinate, defaults to pass of it does not recognize type

    min_att, pass the minimum number of attempts, defualt is 5

    This calculates for all points in tmp1_df, so limit it to the point desired before calling
  '''

  # default 
  el_area = None
  el_message = 'find_ellipse_area: '
  el_success = False
  el_url = ''
  
  if type == 'srv':
    var_x = 'serve_dest_x'
    var_y = 'serve_dest_y'
  elif type == 'pass':
    var_x = 'pass_dest_x'
    var_y = 'pass_dest_y'
  elif type == 'set':
    var_x = 'set_dest_x'
    var_y = 'set_dest_y'
  elif type == 'att':
    var_x = 'att_dest_x'
    var_y = 'att_dest_y'
  elif type == 'dig':
    var_x = 'dig_dest_x'
    var_y = 'pdig_dest_y'
  else:
    # default to pass
    el_message = el_message + 'type mismatch, used pass.  type='+type
    var_x = 'pass_dest_x'
    var_y = 'pass_dest_y'
    
  el_points = pd.concat( [tmp1_df[var_x],tmp1_df[var_y]], axis = 1)
  #print(f" el_points {el_points}")
  el_points = el_points.dropna().values
  el_att = len(el_points)
  if el_att >= min_att:  # must have at least 5 points to calculate the ellipse
    el_message = el_message + ' Ellipse calculated, number of points ='+str(el_att)
    el_mean, el_width, el_height, el_angle  = calculate_standard_deviation_ellipse(el_points, confidence=1.0)

    # not store the ellipse area
    #print(f"Assigning Ellipse Area: points: {el_points}, variable: {fbhe_var_ea}, Height: {type(ellipse_height)}, {ellipse_height}, Width: {type(ellipse_width)}, {ellipse_width}")
    el_area = math.pi*(el_width/2)*(el_height/2)
    el_success = True

  return {
    'area':el_area, 
    'type':type, 
    'message':el_message, 
    'success':el_success,
    'attempts':el_att,
    'URL':el_url
  }


#---------------------------------------------------------------------------
#
#              report to calculate and show the corelations in a league, to wining sets
#
#---------------------------------------------------------------------------
@anvil.server.callable
def league_tri_corr(lgy, team, **rpt_filters):
  '''
  Report Functions:
    - Caluclate and display the correlations of all v ariables int he triangle scoring table w.r.t. point differential

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  #
  # comment out the dataframe not needed for this report
  #
  # for this report, only need the triangle scoring table
  
  #ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  #print(f"PPR DF size, new {ppr_df.shape[0]}")
  #ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  #player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #pair_data_df, pair_data_stats_df = get_pair_data(disp_league, disp_gender, disp_year)
  tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # initiate return lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=inspect.currentframe().f_code.co_name)
  title_list[0] = rpt_row['rpt_title']
  title_list[1] = rpt_row['rpt_sub_title']
  title_list[2] = rpt_row['rpt_section_title1']
  title_list[3] = rpt_filters.get('lgy')
  title_list[4] = rpt_row['team_name']
  title_list[5] = rpt_row['rpt_type']
  title_list[6] = rpt_row['filter_text']
  title_list[7] = rpt_row['explain_text']
  title_list[8] = rpt_filters.get('player')
  title_list[9] = rpt_filters.get('pair')

  label_list[0] = rpt_row['box1_title']
  label_list[1] = rpt_row['box2_title']
  label_list[2] = rpt_row['box3_title']
  label_list[3] = rpt_row['box4_title']
  label_list[4] = rpt_row['box5_title']
  label_list[5] = rpt_row['box6_title']
  label_list[6] = rpt_row['box7_title']
  label_list[7] = rpt_row['box8_title']
  label_list[8] = rpt_row['box9_title']
  label_list[9] = rpt_row['box10_title']

  #------------------------------------------------------------------------------------------------------
  #
  #            Create the images and dataframes with filtered ppr data for report
  #
  #-----------------------------------------------------------------------------------------------------

  '''
  note, this is a league report, so no team or disp_player in this report
  '''
  
  #=====================================================================================
  #-------------------------------------------------------------------------------------
  #
  #     Report is 'set up', now calculate ...
  #
  #-------------------------------------------------------------------------------------
  #=====================================================================================

  # Select only columns containing 'win', 'loser', or 'diff' (case-insensitive)
  filtered_cols = tri_df.filter(regex='(?i)(win|loser|diff)').columns

  # Ensure 'point_diff' is in the filtered columns
  if 'point_diff' not in filtered_cols:
    raise ValueError("Column 'point_diff' not found in filtered columns")

  # Select numeric columns from the filtered set
  numeric_df = tri_df[filtered_cols].select_dtypes(include=['float64', 'int64'])

  # Ensure 'point_diff' is in the numeric DataFrame
  if 'point_diff' not in numeric_df.columns:
    raise ValueError("Column 'point_diff' not found or is not numeric")

  # Option 2: Fill missing values (e.g., with mean)
  numeric_df = numeric_df.fillna(numeric_df.mean())

  #print(f"numberi df size {numeric_df.shape[0]}\n{numeric_df}")
  # Calculate Pearson correlation of all numeric columns with 'point_diff'
  correlations = numeric_df.corrwith(numeric_df['point_diff'])

  # Sort correlations in descending order for better readability
  correlations = correlations.sort_values(ascending=False)

  #print(f" correlations size: {len(correlations)}\n {correlations}")
  
  # Select top 10 positive and negative correlations
  top_corr = pd.concat([correlations.head(10), correlations.tail(10)])

  # Create a bar chart
  plt.figure(figsize=(10, 6))
  top_corr.plot(kind='barh', ax=plt.gca(), legend=False)
  plt.title('Top Correlations with point_diff')
  plt.ylabel('Correlation Coefficient')
  plt.tight_layout()

  # Store the figure in image_list
  image_list[0] = anvil.mpl_util.plot_image()

  # Convert top_corr Series to a DataFrame
  top_corr = top_corr.to_frame(name='Correlation')

  # Add index column by resetting the index
  top_corr = top_corr.reset_index().rename(columns={'index': 'Feature'})
  
  # Create scatter plots for top 4 and bottom 4 variables
  top_4 = top_corr.head(4)['Feature'].tolist()
  bottom_4 = top_corr.tail(4)['Feature'].tolist()
  scatter_vars = top_4 

  # Create a 2x2 grid of subplots for scatter plots
  fig_scatter, axes = plt.subplots(nrows=2, ncols=2, figsize=(16, 8), sharex=True)
  axes = axes.flatten()

  for i, var in enumerate(scatter_vars):
    if var in numeric_df.columns:
      axes[i].scatter(numeric_df['point_diff'], numeric_df[var], alpha=0.5)
      axes[i].set_title(f'{var} vs point_diff')
      axes[i].set_xlabel('point_diff')
      axes[i].set_ylabel(var)
    else:
      axes[i].text(0.5, 0.5, f'{var} not found', ha='center', va='center')
      axes[i].set_axis_off()

  plt.tight_layout()

  # Store the scatter plot figure in image_list[1]
  image_list[1] = anvil.mpl_util.plot_image()

  scatter_vars = bottom_4 

  # Create a 2x2 grid of subplots for scatter plots
  fig_scatter, axes = plt.subplots(nrows=2, ncols=2, figsize=(16, 8), sharex=True)
  axes = axes.flatten()

  for i, var in enumerate(scatter_vars):
    if var in numeric_df.columns:
      axes[i].scatter(numeric_df['point_diff'], numeric_df[var], alpha=0.5)
      axes[i].set_title(f'{var} vs point_diff')
      axes[i].set_xlabel('point_diff')
      axes[i].set_ylabel(var)
    else:
      axes[i].text(0.5, 0.5, f'{var} not found', ha='center', va='center')
      axes[i].set_axis_off()

    plt.tight_layout()

  # Store the scatter plot figure in image_list[1]
  image_list[2] = anvil.mpl_util.plot_image()

  # Store top_corr DataFrame in df_list
  df_list[0] = top_corr.to_dict('records')

  plt.show()
  plt.close('all')


  return title_list, label_list, image_list, df_list
