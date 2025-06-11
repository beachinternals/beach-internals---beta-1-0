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
  title_list[3] = rpt_row['rpt_section_title2']
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
  df_list[0] = cons_table.to_dict('records')
  
  cons2_table, no_data1 = calc_consistency_s2s_table( ppr_df, disp_player )
  df_list[1] = cons_table.to_dict('records')

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
    
  print(f" Summary dataframe: \n {sum_df}")
  
  # now create histograms for each one
  size = [11,5]
  avg_title = disp_league + " Average : "
  plt1 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['FBHE'].tolist(), 'First Ball Hitting Efficiency', '', 'FBHE', size, avg_title, player_data_stats_df.at[0,'fbhe_mean']   )
  plt2 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Errors'].tolist(), 'Error Denisty', '', 'Error Denisty', size, avg_title, player_data_stats_df.at[0,'err_den_mean']/100 )
  plt3 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Transition'].tolist(), 'Transition Conversion', '', 'Transition Conversion', size, avg_title, player_data_stats_df.at[0,'tcr_mean']/100 )
  plt4 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Knockout'].tolist(), 'Serving Aggressiveness', '', 'Serving - Knockout Percent', size, avg_title, player_data_stats_df.at[0,'knockout_mean'] )
  plt5 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Good Pass'].tolist(), 'Passing Quality', '', 'Percent Good Passes', size, avg_title, player_data_stats_df.at[0,'goodpass_mean'] )
  plt6 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Points'].tolist(), 'Percent of Points Won', '', 'Percent of Points Earned', size, '', 0 )

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

  print(f" Summary dataframe: \n {sum_df}")

  
  # now create histograms for each one
  size = [11,5]
  avg_title = disp_league + " Average"
  plt1 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['FBHE'].tolist(), 'First Ball Hitting Efficiency', '', 'FBHE', size, avg_title, pair_data_stats_df.at[0,'fbhe_mean']  )
  plt2 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Errors'].tolist(), 'Error Denisty', '', 'Error Denisty', size, avg_title, pair_data_stats_df.at[0,'err_den_mean']/50 )
  # error density that is stored is a percentage for the player, so time 100 to get to a deciimal, then tiimes two to match for a pair.
  plt3 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Transition'].tolist(), 'Transition Conversion', '', 'Transition Conversion', size, avg_title, pair_data_stats_df.at[0,'tcr_mean']/100 )
  # tcr is stored as a percent, so divide by 100 to match the calculations here
  plt4 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Knockout'].tolist(), 'Serving Aggressiveness', '', 'Serving - Knockout Percent', size, avg_title, pair_data_stats_df.at[0,'knockout_mean'] )
  plt5 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Good Pass'].tolist(), 'Passing Quality', '', 'Percent Good Passes', size, avg_title, pair_data_stats_df.at[0,'goodpass_mean'] )
  plt6 = plot_bar_graph( sum_df['Variable'].tolist(), sum_df['Points'].tolist(), 'Percent of Points Won', '', 'Percent of Points Earned', size, '', 0 )

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
    'Metric': ['League', 'Points', 'Sets', 'Players', 'Pairs', 'Win w/ Hgher FBHE', 'Win w/ Higher Transtiion', 'Win w/ Lower Errors', 'Win w/Higher FBHE & Average Error & Trans'],
    'Number': ['', 0, 0, 0, 0, 0, 0, 0, 0],
    'Percent': ['', '', '', '', '', 0, 0, 0, 0]
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
  
  df_table.at['Win w/ Higher Transtiion','Number'] = tri_df[ (tri_df['win_tcr'] > tri_df['loser_tcr']) ].shape[0]
  df_table.at['Win w/ Higher Transtiion','Percent'] = str('{:.1%}'.format(df_table.at['Win w/ Higher Transtiion','Number']/df_table.at['Sets','Number']))

  # Extract scalar thresholds
  err_den_high = (player_data_stats_df['err_den_mean'].iloc[0] + player_data_stats_df['err_den_stdev'].iloc[0])/100
  tcr_low = (player_data_stats_df['tcr_mean'].iloc[0] + player_data_stats_df['tcr_stdev'].iloc[0])/100

  # Filter tri_df
  tmp_df = tri_df[tri_df['win_err_den'] < err_den_high]
  tmp_df = tmp_df[tmp_df['win_tcr'] > tcr_low]

  df_table.at['Win w/Higher FBHE & Average Error & Trans','Number'] = tmp_df[ (tmp_df['win_fbhe_noace'] >= tmp_df['loser_fbhe_noace'])  ].shape[0]
  if tmp_df.shape[0] == 0:
    df_table.at['Win w/Higher FBHE & Average Error & Trans','Percent'] = 0
  else:
    df_table.at['Win w/Higher FBHE & Average Error & Trans','Percent'] = str('{:.1%}'.format(df_table.at['Win w/Higher FBHE & Average Error & Trans','Number']/tmp_df.shape[0]))

  print(f'reports league: df_table \n {df_table}')
  # put the DF's in the df_list
  df_table = df_table.reset_index()
  df_list[0] = df_table.to_dict('records')


  #------------------------------------------------------------------------------------------------------
  #          Define Images and dataframes for 5 plots
  #------------------------------------------------------------------------------------------------------

  plt_size = [20,15]

  # Image for the Historgram of FBHE
  stat_text, hist_plot = anvil.server.call('plot_histogram',lgy,'fbhe','First Ball Hitting Efficiency', -1, 1)
  image_list[0] = hist_plot
  print(f" stat_text: {stat_text}")
  df = pd.DataFrame({'':[stat_text]})
  df_list[1] = df.to_dict('records')
  
  # Image for the Historgram of Error Density
  stat_text, hist_plot = anvil.server.call('plot_histogram',lgy,'err_den','Error Density', 0, 100)
  image_list[2] = hist_plot
  print(f" stat_text: {stat_text}")
  df = pd.DataFrame({'':[stat_text]})
  df_list[2] = df.to_dict('records')
  
  # Image for the Historgram of Transition Conversion
  stat_text, hist_plot = anvil.server.call('plot_histogram',lgy,'tcr','Transition Conversion', 0, 100)
  image_list[4] = hist_plot
  print(f" stat_text: {stat_text}")
  df = pd.DataFrame({'':[stat_text]})
  df_list[3] = df.to_dict('records')
  
  # Image for the Historgram of Knock Out
  stat_text, hist_plot = anvil.server.call('plot_histogram',lgy,'knockout','Serving Aggressiveness - Knock Out', 0,1)
  image_list[6] = hist_plot
  print(f" stat_text: {stat_text}")
  df = pd.DataFrame({'':[stat_text]})
  df_list[4] = df.to_dict('records')
  
  # Image for the Historgram of Good Pass Percent
  stat_text, hist_plot = anvil.server.call('plot_histogram',lgy,'goodpass','Percent Good Passes', 0.1, 1)
  image_list[8] = hist_plot
  print(f" stat_text: {stat_text}")
  df_list[5] = df.to_dict('records')

  # Image for the Bar Graph of FBHE vs winning %
  plot_df, status = count_wins( tri_df['win_fbhe_noace'], tri_df['loser_fbhe_noace'], -1, 1 )
  bar_plot = plot_bar_graph( plot_df['MidPoint'], plot_df['Win Percent'], 'Winning Percent by FBHE', 'FBHE', 'Percent Wins', plt_size, '', 0, bar_width=0.025)
  image_list[1] = bar_plot

  # Image for the Bar Graph of Error Denisty vs winning %
  plot_df, status = count_wins( tri_df['win_err_den'], tri_df['loser_err_den'], 0, 100 )
  bar_plot = plot_bar_graph( plot_df['MidPoint'], plot_df['Win Percent'], 'Winning Percent by Error Density', 'Error Density', 'Percent Wins', plt_size, '', 0, bar_width=0.025)
  image_list[3] = bar_plot

  # Image for the Bar Graph of TCR vs winning %
  plot_df, status = count_wins( tri_df['win_tcr'], tri_df['loser_tcr'], 0, 100 )
  bar_plot = plot_bar_graph( plot_df['MidPoint'], plot_df['Win Percent'], 'Winning Percent by Transition Conversion Rate', 'Transition Conversion Rate', 'Percent Wins', plt_size, '', 0, bar_width=0.025)
  image_list[5] = bar_plot

  # Image for the Bar Graph of Knock Out vs winning %
  plot_df, status = count_wins( tri_df['win_knockout'], tri_df['loser_knockout'], 0, 1 )
  bar_plot = plot_bar_graph( plot_df['MidPoint'], plot_df['Win Percent'], 'Winning Percent by Knockoout Rate', 'Knockout', 'Percent Wins', plt_size, '', 0, bar_width=0.025)
  image_list[7] = bar_plot

  # Image for the Bar Graph of Good Pass % vs winning %
  plot_df, status = count_wins( tri_df['win_goodpass'], tri_df['loser_goodpass'], 0.1, 1 )
  bar_plot = plot_bar_graph( plot_df['MidPoint'], plot_df['Win Percent'], 'Winning Percent by Good Passes', 'Percent Good Passes', 'Percent Wins', plt_size, '', 0, bar_width=0.025)
  image_list[9] = bar_plot
  

  print(f"Return Values: Title List \n {title_list}")
  print(f"Return Values: Label List \n {label_list}")
  print(f"Return Values: Image List \n {image_list}")
  print(f"Return Values: DF List \n {df_list}")


  
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
  print(f"max, min, and step: Min:{min_value}, Max:{max_value}, Step:{step}")

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

  print(f" df_table :\n {df_table}")
  print(f"Combined df: \n {combined_df}")
  
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

  print(f' df table in count-wins functons: \n{df_table}')
  return df_table, True
  