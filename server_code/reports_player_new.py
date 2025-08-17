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
from matplotlib.patches import Ellipse
import numpy as np
import scipy.stats as stats
from scipy.stats import chi2
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_selection import mutual_info_classif
#import sklearn
import seaborn as sns

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import anvil.media
from io import BytesIO
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

  # Routines to calculate the dataframes and images (plots)

  # not store the dataframes and images in the df_list and image_list to return
  df_list[1] = df.to_dict('records')
  image_list[0] = plt


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
  df_dict = {' ':['FBHE','Percentile','FBSO','ESO','Kills','Errors','Attempts','% In System','Percentile','URL'],
             'All':[0,0,0,0,0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,0,0,0,0,' '],
             'No Zone':[0,0,0,0,0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( ppr_df, disp_player, 'pass', True )
    eso_obj = calc_player_eso(ppr_df,disp_player)
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[4,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[5,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[6,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[2,'All'] = fbhe_vector[4]  # FBSO
    fbhe_table.at[9,'All'] = fbhe_vector[5]  # URL
    fbhe_table.at[3,'All'] = eso_obj.get('eso')  # ESO
    #fbhe_table.at[3,'All'] = float("{:.3f}").format(fbhe_table.at[2,'All'])    
    oos_vector = count_out_of_system( ppr_df, disp_player, 'pass' )
    fbhe_table.at[7,'All'] = 1 - oos_vector[1]  # Good Pass
    fbhe_table.at[7,'All'] = str('{:.1%}').format(fbhe_table.at[7,'All'])
    # FBHE Percentile
    fbhe_table.at[1,'All'] =  round( stats.norm.cdf( (fbhe_vector[0] - player_data_stats_df.at[0,'fbhe_mean'])/ player_data_stats_df.at[0,'fbhe_stdev'] ), 3)
    fbhe_table.at[1,'All'] = str('{:.0%}').format(fbhe_table.at[1,'All'])
    value = fbhe_table.at[7, 'All']  # '89.3%'
    float_value = float(value.replace('%', ''))/100  # 89.3
    fbhe_table.at[8,'All'] =  round( stats.norm.cdf( (float_value - player_data_stats_df.at[0,'goodpass_mean'])/ player_data_stats_df.at[0,'goodpass_stdev'] ), 3)
    fbhe_table.at[8,'All'] = str('{:.0%}').format(fbhe_table.at[8,'All'])
    
    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 3','Zone 5','No Zone']
    for i in [0,1,2,3]:
      zone = 0 if i == 3 else (i*2)+1
      fbhe_vector = fbhe( ppr_df[ppr_df['serve_src_zone_net']==zone], disp_player, 'pass', True )
      eso_obj = calc_player_eso(ppr_df[ppr_df['serve_src_zone_net']==zone],disp_player)
      fbhe_table.at[0,column[i]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[4,column[i]] = fbhe_vector[1]  # attacks
      fbhe_table.at[5,column[i]] = fbhe_vector[2]  # errors
      fbhe_table.at[6,column[i]] = fbhe_vector[3]  # attempts
      fbhe_table.at[2,column[i]] = fbhe_vector[4]  # fbso
      fbhe_table.at[9,column[i]] = fbhe_vector[5]  # URL
      fbhe_table.at[3,column[i]] = eso_obj.get('eso')  # ESO
      #fbhe_table.at[3,column[i]] = float('{:.3f}').format(fbhe_table.at[2,column[i]])
      oos_vector = count_out_of_system( ppr_df[ppr_df['serve_src_zone_net']==zone], disp_player, 'pass' )
      fbhe_table.at[7,column[i]] = 1 - oos_vector[1]  # Good Pass
      fbhe_table.at[7,column[i]] = str('{:.1%}').format(fbhe_table.at[7,column[i]])
      fbhe_table.at[1,column[i]] =  round( stats.norm.cdf( (fbhe_vector[0] - player_data_stats_df.at[0,'fbhe_mean'])/ player_data_stats_df.at[0,'fbhe_stdev'] ), 3)
      fbhe_table.at[1,column[i]] = str('{:.0%}').format(fbhe_table.at[1,column[i]])
      value = fbhe_table.at[7,column[i]]  # '89.3%'
      float_value = float(value.replace('%', ''))/100  # 89.3
      fbhe_table.at[8,column[i]] =  round( stats.norm.cdf( (float_value - player_data_stats_df.at[0,'goodpass_mean'])/ player_data_stats_df.at[0,'goodpass_stdev'] ), 3)
      fbhe_table.at[8,column[i]] = str('{:.0%}').format(fbhe_table.at[8,column[i]])
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
  att1_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  att3_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  att5_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]

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
        att1_val[index] = fbhe_vector[3]
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
        att3_val[index] = fbhe_vector[3]
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
        att5_val[index] = fbhe_vector[3]
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

  # get high and low for the color scheme, mean +/- 2 sdtd
  cmax = player_data_stats_df.at[0,'fbhe_mean']+2*player_data_stats_df.at[0,'fbhe_stdev']
  cmin = player_data_stats_df.at[0,'fbhe_mean']-2*player_data_stats_df.at[0,'fbhe_stdev']
  
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x11, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass1_val, vmin=cmin, vmax=cmax, cmap='PiYG' )  
  # Add title with large font
  ax.set_title("FBHE from Zone 1, Left", fontsize=35)
  z1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass3_val, vmin=cmin, vmax=cmax, cmap='PiYG' ) 
  # Add title with large font
  ax.set_title("FBHE from Zone 3, Middle", fontsize=35)
  z3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass5_val, vmin=cmin, vmax=cmax, cmap='PiYG' )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap='PiYG'),ax=ax, orientation='vertical', label='First Ball Hitting Efficiency')
  # Add title with large font
  ax.set_title("FBHE from Zone 5, Right", fontsize=35)
  z5_plt = anvil.mpl_util.plot_image()

  # put the Images in the image_list
  image_list[3] = z1_plt
  image_list[4] = z3_plt
  image_list[5] = z5_plt
  
  # put the DF's in the df_list
  df_list[0] = fbhe_table.to_dict('records')
  df_list[1] = z1_table.to_dict('records')
  df_list[2] = z3_table.to_dict('records')
  df_list[3] = z5_table.to_dict('records')

  #
  #  now create plots for attempts from zone 1,3,5
  #

  # get high and low for the color scheme, mean +/- 2 sdtd
  cmax = 25  # kind of a guess on the maximum number of attemtps in one of the 46 serves
  cmin = 5  # a logical minimum since we don show anything less then 5 attempts
  
  # from zone 1
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  print(f"Attemtps values 1 {att1_val}")
  ax.plot( [x11, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=att1_val, vmin=cmin, vmax=cmax, cmap='PiYG' ) 
  ax.set_title("Attempts from Zone 1, Left", fontsize=35)
  a1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  print(f"Attemtps values 3 {att3_val}")
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=att3_val, vmin=cmin, vmax=cmax, cmap='PiYG' ) 
  ax.set_title("Attempts from Zone 3, Middle", fontsize=35)
  a3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  print(f"Attemtps values 5 {att5_val}")
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=att5_val, vmin=cmin, vmax=cmax, cmap='PiYG' )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap='PiYG'),ax=ax, orientation='vertical', label='Attempts')
  ax.set_title("Attempts from Zone 5, Right", fontsize=35)
  a5_plt = anvil.mpl_util.plot_image()

  image_list[0] = a1_plt
  image_list[1] = a3_plt
  image_list[2] = a5_plt

  plt.close('All')
  
  return title_list, label_list, image_list, df_list


#---------------------------------------------------------------------------
#
#              player Out of System by 45 Zones
#
#---------------------------------------------------------------------------
@anvil.server.callable
def player_45_passing_new(lgy, team, **rpt_filters):
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

  # firt, this reprot is only when the player is passing, so:
  ppr_df = ppr_df[ppr_df['pass_player'] == disp_player] 
  
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
  df_dict = {' Out of System':['Number','Percent','Percentile','Attempts','URL'],
             'All':[0,0,0,0,' '],
             'Zone 1':[0,0,0,0,' '],
             'Zone 3':[0,0,0,0,' '],
             'Zone 5':[0,0,0,0,' '],
             'No Zone':[0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )
  df_dict1 = {' Pass Area':['Area','Percentile','Attempts','URL'],
             'All':[0,0,0,' '],
             'Zone 1':[0,0,0,' '],
             'Zone 3':[0,0,0,' '],
             'Zone 5':[0,0,0,' '],
             'No Zone':[0,0,0,' ']
            }
  area_table = pd.DataFrame.from_dict( df_dict1 )

  # storing the area so I can get max and min for the graph
  el_area = []
  
  # oos_vector = count_out_of_system(ppr_df,disp_player,action)
  # action is 'pass', 'att', 'srv'
  # returns a vector : oos_vector[0] = number OOS (int), oos_vector[1] = percent out of system (Float()), oos_vector[2] = attempts (int())
  
  ############### Third Populate the dataframe, assuming we have data returned
  if ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = count_out_of_system(ppr_df, disp_player, 'pass')
    fbhe_table.at[0,'All'] = fbhe_vector[0]  #number out of system,
    fbhe_table.at[1,'All'] = str('{:.1%}').format(fbhe_vector[1])  # percent out of system
    fbhe_table.at[2,'All'] = round( stats.norm.cdf((((1-fbhe_vector[1])- player_data_stats_df.at[0,'goodpass_mean'])/(player_data_stats_df.at[0,'goodpass_stdev']))) , 3)
    fbhe_table.at[2,'All'] = str('{:.0%}').format(fbhe_table.at[2,'All'])
    fbhe_table.at[3,'All'] = fbhe_vector[2]  # attempts
    #fbhe_table.at[3,'All'] = fbhe_vector[3]  # URL (someday?)

    el_result = find_ellipse_area(ppr_df, 'pass', min_att=5)
    if el_result.get('attempts') >= 5:
      area_table.at[0,'All'] = str('{:.1f}').format(el_result.get('area'))
      area_table.at[2,'All'] = el_result.get('attempts')
      #area_table.at[1,'All'] = round( ((el_result.get('area')- player_data_stats_df.at[0,'pass_ea_mean'])/(player_data_stats_df.at[0,'pass_es_stdev'])) , 3)
      area_table.at[3,'All'] = el_result.get('URL')  
      el_area.append(el_result.get('area'))

      
    
    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 3','Zone 5','No Zone']
    for i in [0,1,2,3]:
      zone = 0 if i == 3 else (i*2)+1
      fbhe_vector = count_out_of_system( ppr_df[ppr_df['serve_src_zone_net']==zone], disp_player, 'pass')
      fbhe_table.at[0,column[i]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i]] = str('{:.1%}').format(fbhe_vector[1])  # attacks
      fbhe_table.at[2,column[i]] = round( stats.norm.cdf((((1-fbhe_vector[1])- player_data_stats_df.at[0,'goodpass_mean'])/(player_data_stats_df.at[0,'goodpass_stdev']))) , 3)
      fbhe_table.at[2,column[i]] = str('{:.0%}').format(fbhe_table.at[2,column[i]])
      fbhe_table.at[3,column[i]] = fbhe_vector[2]  # errors
      #fbhe_table.at[3,column[i]] = fbhe_vector[3]  # URL someday

      el_result = find_ellipse_area(ppr_df[ppr_df['serve_src_zone_net']==zone], 'pass', min_att=5)
      if el_result.get('attempts') >= 5:
        area_table.at[0,column[i]] = str('{:.1f}').format(el_result.get('area'))
        #area_table.at[1,column[i]] = round( ((el_result.get('area')- player_data_stats_df.at[0,'pass_ea_mean'])/(player_data_stats_df.at[0,'pass_es_stdev'])) , 3)
        area_table.at[2,column[i]] = el_result.get('attempts')
        area_table.at[3,column[i]] = el_result.get('URL')
        el_area.append(el_result.get('area'))



  else:
    fbhe_table.at[0,'All'] = "No Data Found"
    area_table.at[0,'All'] = "No Data Found"

  
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
  area1_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  area3_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  area5_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]

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
  df_dict9 = {'Dest Zone':[' '],
             'Area':[0],
             'Att':[0],
             'URL':[' ']
            }
  a1_table = pd.DataFrame.from_dict( df_dict9 )
  a3_table = pd.DataFrame.from_dict( df_dict9 )
  a5_table = pd.DataFrame.from_dict( df_dict9 )
  a1_table_index = 0
  a3_table_index = 0
  a5_table_index = 0

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

        # Zone 1 Area
      el_result = find_ellipse_area(ppr_df[  (ppr_df['serve_src_zone_net'] == 1) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                                    'pass', min_att=5
                                   )
      print(f"el result for zone 1: attempts: {el_result.get('attempts')}, area: {el_result.get('area')}")
      if el_result.get('attempts') >= 5:
        area1_val[index] = el_result.get('area')
        a1_table.loc[a1_table_index,'Dest Zone'] = str(i)+j.capitalize()
        a1_table.loc[a1_table_index,'Area'] = str('{:.1f}').format(el_result.get('area'))
        a1_table.loc[a1_table_index,'Att'] = el_result.get('attempts')
        a1_table.loc[a1_table_index,'URL'] = el_result.get('URL')
        el_area.append(el_result.get('area'))
        a1_table_index = a1_table_index + 1
        
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

        # Zone 3 Area
      el_result = find_ellipse_area(ppr_df[  (ppr_df['serve_src_zone_net'] == 3) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                                    'pass', min_att=5
                                   )
      #print(f"el result for zone 1: attempts: {el_result.get('attempts')}, area: {el_result.get('area')}")
      if el_result.get('attempts') >= 5:
        area3_val[index] = el_result.get('area')
        a3_table.loc[a3_table_index,'Dest Zone'] = str(i)+j.capitalize()
        a3_table.loc[a3_table_index,'Area'] = str('{:.1f}').format(el_result.get('area'))
        a3_table.loc[a3_table_index,'Att'] = el_result.get('attempts')
        a3_table.loc[a3_table_index,'URL'] = el_result.get('URL')
        el_area.append(el_result.get('area'))
        a3_table_index = a3_table_index + 1

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

        # Zone 5 Area
      el_result = find_ellipse_area(ppr_df[  (ppr_df['serve_src_zone_net'] == 5) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                                    'pass', min_att=5
                                   )
      #print(f"el result for zone 1: attempts: {el_result.get('attempts')}, area: {el_result.get('area')}")
      if el_result.get('attempts') >= 5:
        area5_val[index] = el_result.get('area')
        a5_table.loc[a5_table_index,'Dest Zone'] = str(i)+j.capitalize()
        a5_table.loc[a5_table_index,'Area'] = str('{:.1f}').format(el_result.get('area'))
        a5_table.loc[a5_table_index,'Att'] = el_result.get('attempts')
        a5_table.loc[a5_table_index,'URL'] = el_result.get('URL')
        el_area.append(el_result.get('area'))
        a5_table_index = a5_table_index + 1

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
  ax.set_title("Percent Good Passes from Zone 1, Left", fontsize=35)
  z1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass3_val, vmin=cmin, vmax=cmax, cmap=custom_cmap ) 
  ax.set_title("Percent Good Passes from Zone 3, Middle", fontsize=35)
  z3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass5_val, vmin=cmin, vmax=cmax, cmap=custom_cmap )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap=custom_cmap),ax=ax, orientation='vertical', label='Percent Good Passes')
  ax.set_title("Percent Good Passes from Zone 5, Right", fontsize=35)
  z5_plt = anvil.mpl_util.plot_image()

  cmin = min(el_area)
  if cmin < 0:
    cmin = 0
  cmax = max(el_area)
  if cmax > 20:
    cmax = 20
  
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x11, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=area1_val, vmin=cmin, vmax=cmax, cmap=custom_cmap )  
  ax.set_title("Pass Area from Zone 1, Left", fontsize=40)
  a1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=area3_val, vmin=cmin, vmax=cmax, cmap=custom_cmap ) 
  ax.set_title("Pass Area from Zone 3, Middle", fontsize=35)
  a3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=area5_val, vmin=cmin, vmax=cmax, cmap=custom_cmap )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap=custom_cmap),ax=ax, orientation='vertical', label='Pass Area')
  ax.set_title("Pass Area from Zone 5, Right", fontsize=35)
  a5_plt = anvil.mpl_util.plot_image()

  # put the Images in the image_list
  image_list[0] = z1_plt
  image_list[1] = z3_plt
  image_list[2] = z5_plt
  image_list[3] = a1_plt
  image_list[4] = a3_plt
  image_list[5] = a5_plt

  # put the DF's in the df_list
  df_list[0] = fbhe_table.to_dict('records')
  df_list[1] = z1_table.to_dict('records')
  df_list[2] = z3_table.to_dict('records')
  df_list[3] = z5_table.to_dict('records')
  df_list[4] = area_table.to_dict('records')
  df_list[5] = a1_table.to_dict('records')
  df_list[6] = a3_table.to_dict('records')
  df_list[7] = a5_table.to_dict('records') 

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
    print(f" Return from plot 1: {plot1_return}")
    print(f" Return from plot 1, stat text: {plot1_return.get('stat_text')}, media list: {plot1_return.get('media_list')}")
    plot2_return = plot_pass_clusters(ppr_df, disp_player, 'FBE')
    print(f" Return from plot 2: {plot2_return}")
    print(f" Return from plot 2, stat text: {plot2_return.get('stat_text')}, media list: {plot2_return.get('media_list')}")
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
  
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_save = ppr_df
  #print(f"PPR DF size, new {ppr_df.shape[0]}")
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
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

  ##
  ##
  ## Fist step ... correlation analuysis of hte Tri_data file
  ##
  ##
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

  print(f"numberi df size {numeric_df.shape[0]}\n{numeric_df}")
  # Calculate Pearson correlation of all numeric columns with 'point_diff'
  correlations = numeric_df.corrwith(numeric_df['point_diff'])

  # Sort correlations in descending order for better readability
  correlations = correlations.sort_values(ascending=True)

  correlations = correlations.drop('point_diff', errors='ignore')
  
  # Round the 'Correlation' and 'P-value' columns to 3 decimal places
  correlations = correlations.round(3)
  correlations = correlations.dropna()
  
  top_corr = pd.concat([correlations.head(15), correlations.tail(15)])
  print(f" correlations size: {len(correlations)}\n {correlations}")

  fig_size = [15,15]
  # Create a bar chart
  plt.figure(figsize=fig_size)
  top_corr.plot(kind='barh', ax=plt.gca(), legend=False)
  plt.title('Top Correlations with Point Differntial')
  plt.ylabel('Correlation Coefficient')
  plt.tight_layout()

  # Store the figure in image_list
  image_list[0] = anvil.mpl_util.plot_image()

  # Convert top_corr Series to a DataFrame
  top_corr = top_corr.to_frame(name='Correlation')

  # Add index column by resetting the index
  top_corr = top_corr.reset_index().rename(columns={'index': 'Metric'})
  
  # Create scatter plots for top 4 and bottom 4 variables
  top_4 = top_corr.head(4)['Metric'].tolist()
  bottom_4 = top_corr.tail(4)['Metric'].tolist()
  scatter_vars = top_4 +bottom_4

  # Create a 2x2 grid of subplots for scatter plots
  fig_scatter, axes = plt.subplots(nrows=4, ncols=2, figsize=fig_size, sharex=True)
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
  plt.show()
  plt.close('all')
  
  # limit top_corr to top and bottom 5
  top_corr = pd.concat([top_corr.head(10), top_corr.tail(10)])
  top_corr = top_corr.sort_values(by='Correlation',ascending=False)
  df_list[0] = top_corr.to_dict('records')


  ##-----------------------------------------------------------------------------------------
  ##
  ## Second step ... correlation analysis of the player data file
  ##
  ##----------------------------------------------------------------------------------------
  # Select only columns containing not includeing '_n' fields
  filtered_cols = player_data_df.filter(regex='^(?!.*_n$)').columns
  
  # Select numeric columns from the filtered set
  numeric_df = player_data_df[filtered_cols].select_dtypes(include=['float64', 'int64'])

  # Ensure 'point_per' is in the numeric DataFrame
  if 'point_per' not in numeric_df.columns:
    raise ValueError("Column 'point_per' not found or is not numeric")

  # Option 2: Fill missing values (e.g., with mean)
  numeric_df = numeric_df.fillna(numeric_df.mean())

  #print(f"numberi df size {numeric_df.shape[0]}\n{numeric_df}")
  # Calculate Pearson correlation of all numeric columns with 'point_diff'
  correlations = numeric_df.corrwith(numeric_df['point_per'])

  # Sort correlations in descending order for better readability
  correlations = correlations.dropna() # in the player_data_df, we will get a lot of NA because the 45 serves, many are na, blank
  correlations = correlations.sort_values(ascending=True)

  correlations = correlations.drop('point_per', errors='ignore')
  
  # Round the 'Correlation' and 'P-value' columns to 3 decimal places
  correlations = correlations.round(3)

  print(f" correlations size: {len(correlations)}\n {correlations}")

  # Select top 10 positive and negative correlations
  top_corr = pd.concat([correlations.head(15), correlations.tail(15)])

  fig_size = [15,15]
  # Create a bar chart
  plt.figure(figsize=fig_size)
  top_corr.plot(kind='barh', ax=plt.gca(), legend=False)
  plt.title('Top Correlations with Point Percentage')
  plt.ylabel('Correlation Coefficient')
  plt.tight_layout()

  # Store the figure in image_list
  image_list[2] = anvil.mpl_util.plot_image()

  # Convert top_corr Series to a DataFrame
  top_corr = top_corr.to_frame(name='Correlation')

  # Add index column by resetting the index
  top_corr = top_corr.reset_index().rename(columns={'index': 'Metric'})

  # Create scatter plots for top 4 and bottom 4 variables
  top_4 = top_corr.head(4)['Metric'].tolist()
  bottom_4 = top_corr.tail(4)['Metric'].tolist()
  scatter_vars = top_4 +bottom_4

  # Create a 2x2 grid of subplots for scatter plots
  fig_scatter, axes = plt.subplots(nrows=4, ncols=2, figsize=fig_size, sharex=True)
  axes = axes.flatten()

  for i, var in enumerate(scatter_vars):
    if var in numeric_df.columns:
      axes[i].scatter(numeric_df['point_per'], numeric_df[var], alpha=0.5)
      axes[i].set_title(f'{var} vs point_per')
      axes[i].set_xlabel('Percentage of Points')
      axes[i].set_ylabel(var)
    else:
      axes[i].text(0.5, 0.5, f'{var} not found', ha='center', va='center')
      axes[i].set_axis_off()

  plt.tight_layout()

  # Store the scatter plot figure in image_list[1]
  image_list[3] = anvil.mpl_util.plot_image()
  plt.show()
  plt.close('all')

  # Store top_corr DataFrame in df_list
  # limit top_corr to top and bottom 5
  top_corr = pd.concat([top_corr.head(10), top_corr.tail(10)])
  top_corr = top_corr.sort_values(by='Correlation',ascending=False)
  df_list[1] = top_corr.to_dict('records')

  ##-----------------------------------------------------------------------------------------
  ##
  ## Third step ... correlation analysis of the ppr file with point outcome, where FBK = 1, TK,TE = 0, FBE = -1
  ##
  ##----------------------------------------------------------------------------------------

  # Assuming ppr_df is your dataframe
  ppr_df = ppr_save

  # Step 1: Clean column names
  ppr_df.columns = [col.replace(' ', '_').replace('.', '_').replace(':', '_') for col in ppr_df.columns]
  ppr_df = ppr_df.loc[:, ~ppr_df.columns.str.contains('^Unnamed')]

  # Limit the Dataframe to only these plays, remove all TSE and TSA
  ppr_df = ppr_df[ ppr_df['point_outcome'] != 'TSA']
  ppr_df = ppr_df[ ppr_df['point_outcome'] != 'TSE']
  
  # Step 2: Replace point_outcome values as integers
  ppr_df['point_outcome'] = ppr_df['point_outcome'].replace({
    'FBK': 1,
    'FBE': -1,
    'TK': 0,
    'TE': 0
  }).fillna(np.nan)  # Handle unmapped values
  
  # Convert to integer, handling NaN (converts to float64 if NaN exists)
  ppr_df['point_outcome'] = pd.to_numeric(ppr_df['point_outcome'], errors='coerce')
  # If you want to drop rows with NaN in point_outcome to ensure int64
  ppr_df = ppr_df.dropna(subset=['point_outcome'])
  ppr_df['point_outcome'] = ppr_df['point_outcome'].astype('int64')

  # Verify replacement
  print("Updated point_outcome values FBK & FBE:")
  print(ppr_df['point_outcome'].value_counts())

  # Step 3: Limit ppr_df to numerical columns
  ppr_df = ppr_df.select_dtypes(include=['int64', 'float64'])
  print("\nColumns in ppr_df after limiting to numerical:")
  #print(ppr_df.columns.tolist())

  # Step 4: Define desired columns (numerical only)
  desired_cols = [
    'serve_src_x', 'serve_src_y', 'serve_src_zone_net', 'serve_dest_x', 'serve_dest_y',
    'serve_dest_zone_depth', 'serve_dest_zone_net', 'serve_dist', 'serve_dur', 'serve_speed',
    'serve_angle', 'serve_height', 'pass_src_x', 'pass_src_y', 'pass_src_zone_depth',
    'pass_src_zone_net', 'pass_dest_x', 'pass_dest_y', 'pass_dest_zone_depth',
    'pass_dest_zone_net', 'pass_dist', 'pass_dur', 'pass_speed', 'pass_angle', 'pass_height',
    'pass_rtg_btd', 'pass_oos', 'set_src_x', 'set_src_y', 'set_src_zone_depth',
    'set_src_zone_net', 'set_dest_x', 'set_dest_y', 'set_dest_zone_depth',
    'set_dest_zone_net', 'set_dist', 'set_dur', 'set_speed', 'set_angle', 'set_height',
    'att_src_x', 'att_src_y', 'att_src_zone_depth', 'att_src_zone_net', 'att_dest_x',
    'att_dest_y', 'att_dest_zone_depth', 'att_dest_zone_net', 'att_dist', 'att_dur',
    'att_speed', 'att_angle', 'att_height', 'att_touch_height', 'dig_src_x', 'dig_src_y',
    'dig_src_zone_depth', 'dig_src_zone_net', 'dig_dest_x', 'dig_dest_y',
    'dig_dest_zone_depth', 'dig_dest_zone_net', 'dig_dist', 'dig_dur', 'dig_speed',
    'dig_angle', 'dig_height', 'point_outcome'
  ]

  # Filter numerical columns from desired_cols
  numerical_cols = [col for col in desired_cols if col in ppr_df.columns]
  #print(f"\nNumerical columns used for analysis: {numerical_cols}")

  # Step 5: Clean data (handle inf/large values)
  #print("\nChecking for infinite or large values:")
  for col in numerical_cols:
    inf_count = np.isinf(ppr_df[col]).sum()
    large_count = (np.abs(ppr_df[col]) > 1e308).sum()
    nan_count = ppr_df[col].isna().sum()
    #print(f"{col}: {inf_count} infinite, {large_count} too large, {nan_count} NaN")
    ppr_df[col] = ppr_df[col].replace([np.inf, -np.inf], np.nan)
    if not ppr_df[col].isna().all():
      max_val = ppr_df[col].quantile(0.99, interpolation='nearest')
      ppr_df[col] = ppr_df[col].clip(upper=max_val, lower=-max_val)
      ppr_df[col] = ppr_df[col].fillna(ppr_df[col].median())

  # Step 6: Pearson Correlation with point_outcome
  corr_results = []
  for col in numerical_cols:
    if col != 'point_outcome':  # Exclude point_outcome itself
      valid_data = ppr_df[[col, 'point_outcome']].dropna()
      if len(valid_data) > 1 and valid_data['point_outcome'].nunique() > 1:
        corr, p_value = stats.pearsonr(valid_data[col], valid_data['point_outcome'])
        corr_results.append({
          'Metric': col,
          'Correlation': corr,
          'P-Value': p_value,
          'Note': ''
        })
      else:
        corr_results.append({
          'Metric': col,
          'Correlation': None,
          'P-Value': None,
          'Note': 'Insufficient data or single category'
        })

  corr_results_df = pd.DataFrame(corr_results)
  corr_results_df = corr_results_df.sort_values(by='Correlation', ascending=False)

  # Round the 'Correlation' and 'P-value' columns to 3 decimal places
  corr_results_df['Correlation'] = corr_results_df['Correlation'].round(3)
  corr_results_df['P-Value'] = corr_results_df['P-Value'].round(3)
  
  #print("\nPearson Correlation with point_outcome:")
  #print(corr_results_df.sort_values(by='Correlation', ascending=False))

  # Step 7: Visualize significant correlations (bar plot)
  significant_metrics = corr_results_df[corr_results_df['P-Value'] < 0.05]['Metric']
  if not significant_metrics.empty:
    plt.figure(figsize=(fig_size))
    sns.barplot(x='Correlation', y='Metric', data=corr_results_df[corr_results_df['P-Value'] < 0.05])
    plt.title('Significant Correlations with point_outcome')
    plt.show()
    
  image_list[4] = anvil.mpl_util.plot_image()

  # Create scatter plots for top 4 and bottom 4 variables
  top_4 = significant_metrics.head(4)
  bottom_4 = significant_metrics.tail(4)
  scatter_vars = pd.concat([top_4, bottom_4])
  
  # Create a 2x2 grid of subplots for scatter plots
  fig_scatter, axes = plt.subplots(nrows=4, ncols=2, figsize=fig_size, sharex=True)
  axes = axes.flatten()

  for i, var in enumerate(scatter_vars):
    if var in   corr_results_df.columns:
      axes[i].scatter(corr_results_df['point_outcome'], corr_results_df[var], alpha=0.5)
      axes[i].set_title(f'{var} vs Point Outcome FBE (-1) -> FBK (1)')
      axes[i].set_xlabel('Percentage of Points')
      axes[i].set_ylabel(var)
    else:
      axes[i].text(0.5, 0.5, f'{var} not found', ha='center', va='center')
      axes[i].set_axis_off()

  plt.tight_layout()
  image_list[5] = anvil.mpl_util.plot_image()
  
  corr_results_df = corr_results_df[ corr_results_df['P-Value'] < 0.05]
  corr_results_df = pd.concat([corr_results_df.head(10), corr_results_df.tail(10)])
  corr_results_df = corr_results_df.sort_values(by='Correlation', ascending=False)
  df_list[2] = corr_results_df.to_dict('records')


  ##-----------------------------------------------------------------------------------------
  ##
  ## Fourth step ... correlation analysis of the ppr file with point outcome, where TSA = 1, TSE = -1
  ##
  ##----------------------------------------------------------------------------------------

  # Assuming ppr_df is your dataframe
  # Step 1: Clean column names
  ppr_df = ppr_save
  ppr_df.columns = [col.replace(' ', '_').replace('.', '_').replace(':', '_') for col in ppr_df.columns]
  ppr_df = ppr_df.loc[:, ~ppr_df.columns.str.contains('^Unnamed')]

  # Limit the Dataframe to only these plays, only TSA and TSE
  ppr_df = ppr_df[ (ppr_df['point_outcome'] == 'TSA') | ( ppr_df['point_outcome'] != 'TSE' )]

  # Step 2: Replace point_outcome values as integers
  ppr_df['point_outcome'] = ppr_df['point_outcome'].replace({
    'TSA': 1,
    'TSE': -1,
    'TK': 0,
    'TE': 0,
    'FBK': 0,
    'FBE': 0
  }).fillna(np.nan)  # Handle unmapped values

  # Convert to integer, handling NaN (converts to float64 if NaN exists)
  ppr_df['point_outcome'] = pd.to_numeric(ppr_df['point_outcome'], errors='coerce')
  # If you want to drop rows with NaN in point_outcome to ensure int64
  ppr_df = ppr_df.dropna(subset=['point_outcome'])
  ppr_df['point_outcome'] = ppr_df['point_outcome'].astype('int64')

  # Verify replacement
  #print("Updated point_outcome values TSA & TSE:")
  #print(ppr_df['point_outcome'].value_counts())

  # Step 3: Limit ppr_df to numerical columns
  ppr_df = ppr_df.select_dtypes(include=['int64', 'float64'])
  #print("\nColumns in ppr_df after limiting to numerical:")
  #print(ppr_df.columns.tolist())

  # Step 4: Define desired columns (numerical only)
  desired_cols = [
    'serve_src_x', 'serve_src_y', 'serve_src_zone_net', 'serve_dest_x', 'serve_dest_y',
    'serve_dest_zone_depth', 'serve_dest_zone_net', 'serve_dist', 'serve_dur', 'serve_speed',
    'serve_angle', 'serve_height', 'pass_src_x', 'pass_src_y', 'pass_src_zone_depth',
    'pass_src_zone_net', 'pass_dest_x', 'pass_dest_y', 'pass_dest_zone_depth',
    'pass_dest_zone_net', 'pass_dist', 'pass_dur', 'pass_speed', 'pass_angle', 'pass_height',
    'pass_rtg_btd', 'pass_oos', 'set_src_x', 'set_src_y', 'set_src_zone_depth',
    'set_src_zone_net', 'set_dest_x', 'set_dest_y', 'set_dest_zone_depth',
    'set_dest_zone_net', 'set_dist', 'set_dur', 'set_speed', 'set_angle', 'set_height',
    'att_src_x', 'att_src_y', 'att_src_zone_depth', 'att_src_zone_net', 'att_dest_x',
    'att_dest_y', 'att_dest_zone_depth', 'att_dest_zone_net', 'att_dist', 'att_dur',
    'att_speed', 'att_angle', 'att_height', 'att_touch_height', 'dig_src_x', 'dig_src_y',
    'dig_src_zone_depth', 'dig_src_zone_net', 'dig_dest_x', 'dig_dest_y',
    'dig_dest_zone_depth', 'dig_dest_zone_net', 'dig_dist', 'dig_dur', 'dig_speed',
    'dig_angle', 'dig_height', 'point_outcome'
  ]

  # Filter numerical columns from desired_cols
  numerical_cols = [col for col in desired_cols if col in ppr_df.columns]
  #print(f"\nNumerical columns used for analysis: {numerical_cols}")

  # Step 5: Clean data (handle inf/large values)
  #print("\nChecking for infinite or large values:")
  for col in numerical_cols:
    inf_count = np.isinf(ppr_df[col]).sum()
    large_count = (np.abs(ppr_df[col]) > 1e308).sum()
    nan_count = ppr_df[col].isna().sum()
    #print(f"{col}: {inf_count} infinite, {large_count} too large, {nan_count} NaN")
    ppr_df[col] = ppr_df[col].replace([np.inf, -np.inf], np.nan)
    if not ppr_df[col].isna().all():
      max_val = ppr_df[col].quantile(0.99, interpolation='nearest')
      ppr_df[col] = ppr_df[col].clip(upper=max_val, lower=-max_val)
      ppr_df[col] = ppr_df[col].fillna(ppr_df[col].median())

  # Step 6: Pearson Correlation with point_outcome
  corr_results = []
  for col in numerical_cols:
    if col != 'point_outcome':  # Exclude point_outcome itself
      valid_data = ppr_df[[col, 'point_outcome']].dropna()
      if len(valid_data) > 1 and valid_data['point_outcome'].nunique() > 1:
        corr, p_value = stats.pearsonr(valid_data[col], valid_data['point_outcome'])
        corr_results.append({
          'Metric': col,
          'Correlation': corr,
          'P-Value': p_value,
          'Note': ''
        })
      else:
        corr_results.append({
          'Metric': col,
          'Correlation': None,
          'P-Value': None,
          'Note': 'Insufficient data or single category'
        })

  corr_results_df = pd.DataFrame(corr_results)
  corr_results_df = corr_results_df.sort_values(by='Correlation', ascending=False)
  
  # Round the 'Correlation' and 'P-value' columns to 3 decimal places
  corr_results_df['Correlation'] = corr_results_df['Correlation'].round(3)
  corr_results_df['P-Value'] = corr_results_df['P-Value'].round(3)
  
  #print("\nPearson Correlation with point_outcome:")
  #print(corr_results_df.sort_values(by='Correlation', ascending=False))

  # Step 7: Visualize significant correlations (bar plot)
  significant_metrics = corr_results_df[corr_results_df['P-Value'] < 0.05]['Metric']
  if not significant_metrics.empty:
    plt.figure(figsize=(fig_size))
    sns.barplot(x='Correlation', y='Metric', data=corr_results_df[corr_results_df['P-Value'] < 0.05])
    plt.title('Significant Correlations with point_outcome')
    plt.show()
  image_list[6] = anvil.mpl_util.plot_image()


  # Create scatter plots for top 4 and bottom 4 variables
  top_4 = significant_metrics.head(4)
  bottom_4 = significant_metrics.tail(4)
  scatter_vars = pd.concat([top_4, bottom_4])
  #print(f"Scatter Vars: {scatter_vars}")

  # Create a 2x2 grid of subplots for scatter plots
  fig_scatter, axes = plt.subplots(nrows=4, ncols=2, figsize=fig_size, sharex=True)
  axes = axes.flatten()

  for i, var in enumerate(scatter_vars):
    if var in   corr_results_df.columns:
      axes[i].scatter(corr_results_df['point_outcome'], corr_results_df[var], alpha=0.5)
      axes[i].set_title(f'{var} vs Point Outcome FBE (-1) -> FBK (1)')
      axes[i].set_xlabel('Percentage of Points')
      axes[i].set_ylabel(var)
    else:
      axes[i].text(0.5, 0.5, f'{var} not found', ha='center', va='center')
      axes[i].set_axis_off()

  plt.tight_layout()
  image_list[7] = anvil.mpl_util.plot_image()
  
  corr_results_df = corr_results_df[ corr_results_df['P-Value'] < 0.05]
  corr_results_df = pd.concat([corr_results_df.head(10), corr_results_df.tail(10)])
  corr_results_df = corr_results_df.sort_values(by='Correlation', ascending=False)
  df_list[3] = corr_results_df.to_dict('records')


  
  '''
  ##-----------------------------------------------------------------------------------------
  ##
  ## Third step ... correlation analysis of the ppr file when pointoutcome is FBK
  ##
  ##----------------------------------------------------------------------------------------

  # Assuming ppr_df is your dataframe
  # Step 1: Clean column names
  # Replace special characters (e.g., periods, spaces, colons) with underscores
  ppr_df.columns = [col.replace(' ', '_').replace('.', '_').replace(':', '_') for col in ppr_df.columns]

  # Drop any 'Unnamed' columns (e.g., Unnamed: 0_1)
  ppr_df = ppr_df.loc[:, ~ppr_df.columns.str.contains('^Unnamed')]

  # Step 2: Identify numerical columns
  numerical_cols = ppr_df.select_dtypes(include=['int64', 'float64']).columns

  # instead, we will list explicitly which columns to cinlude in the analysis
  desired_cols = {
    'serve_src_x',
    'serve_src_y',
    #'serve_src_zone_net',
    'serve_dest_x',
    'serve_dest_y',
    #'serve_dest_zone_depth',
    #'serve_dest_zone_net',
    'serve_dist',
    'serve_dur',
    'serve_speed',
    'serve_angle',
    'serve_height',
    'pass_src_x',
    'pass_src_y',
    #'pass_src_zone_depth',
    #'pass_src_zone_net',
    'pass_dest_x',
    'pass_dest_y',
    #'pass_dest_zone_depth',
    #'pass_dest_zone_net',
    'pass_dist',
    'pass_dur',
    'pass_speed',
    'pass_angle',
    'pass_height',
    #'pass_rtg_btd',
    'pass_oos',
    #pass_touch_position
    #pass_touch_type
    'set_src_x',
    'set_src_y',
    #'set_src_zone_depth',
    #'set_src_zone_net',
    'set_dest_x',
    'set_dest_y',
    #'set_dest_zone_depth',
    #'set_dest_zone_net',
    'set_dist',
    'set_dur',
    'set_speed',
    'set_angle',
    'set_height',
    #set_touch_type
    'att_src_x',
    'att_src_y',
    #'att_src_zone_depth',
    #'att_src_zone_net',
    'att_dest_x',
    'att_dest_y',
    #'att_dest_zone_depth',
    #'att_dest_zone_net',
    'att_dist',
    'att_dur',
    'att_speed',
    'att_angle',
    'att_height',
    'att_touch_height',
    'dig_src_x',
    'dig_src_y',
    #'dig_src_zone_depth',
    #'dig_src_zone_net',
    'dig_dest_x',
    'dig_dest_y',
    #'dig_dest_zone_depth',
    #'dig_dest_zone_net',
    'dig_dist',
    'dig_dur',
    'dig_speed',
    'dig_angle',
    'dig_height'
    #'point_outcome',
    #'point_outcome_team',
    #tactic
  }
  
  # Step 3: Diagnose infinite/large values and clean data
  print("Checking for infinite or large values in numerical columns:")
  ppr_df_clean = ppr_df.copy()
  for col in desired_cols:
    print(f"Desired Column: {col}")
    inf_count = np.isinf(ppr_df_clean[col]).sum()
    large_count = (np.abs(ppr_df_clean[col]) > 1e308).sum()
    nan_count = ppr_df_clean[col].isna().sum()
    print(f"{col}: {inf_count} infinite, {large_count} too large, {nan_count} NaN")
    # Replace inf/-inf with NaN
    ppr_df_clean[col] = ppr_df_clean[col].replace([np.inf, -np.inf], np.nan)
    # Cap values at 99th percentile (of non-NaN values)
    if not ppr_df_clean[col].isna().all():
      max_val = ppr_df_clean[col].quantile(0.99, interpolation='nearest')
      ppr_df_clean[col] = ppr_df_clean[col].clip(upper=max_val, lower=-max_val)
    
  # Step 4: Kruskal-Wallis test for each numerical column vs. point_outcome
  results = []
  for col in desired_cols:
    # Group data by point_outcome
    groups = [ppr_df[ppr_df['point_outcome'] == outcome][col].dropna() 
              for outcome in ppr_df['point_outcome'].unique()]
    
    # Diagnose group sizes and variance
    group_sizes = [len(g) for g in groups]
    group_variances = [g.var() if len(g) > 1 else 0 for g in groups]
    note = ""
    if len(groups) < 2:
      note = "Only one group (need multiple categories)"
    elif any(len(g) == 0 for g in groups):
      note = f"Empty group(s): {group_sizes}"
    elif any(len(g) == 1 for g in groups):
      note = f"Single-value group(s): {group_sizes}"
    elif any(v == 0 for g, v in zip(groups, group_variances) if len(g) > 1):
      note = f"No variance in group(s): {group_variances}"

    # Perform Kruskal-Wallis if valid
    if not note and all(len(g) > 0 for g in groups) and len(groups) > 1:
      try:
        h_stat, p_value = stats.kruskal(*groups)
        n_total = sum(len(g) for g in groups)
        k = len(groups)
        epsilon_squared = (h_stat - k + 1) / (n_total - k) if n_total > k else 0
        results.append({
          'Metric': col,
          'H-Statistic': h_stat,
          'P-Value': p_value,
          'Epsilon-Squared': epsilon_squared,
          'Note': ''
        })
      except ValueError as e:
        results.append({
          'Metric': col,
          'H-Statistic': None,
          'P-Value': None,
          'Epsilon-Squared': None,
          'Note': f'Error: {str(e)}'
        })
    else:
      results.append({
        'Metric': col,
        'H-Statistic': None,
        'P-Value': None,
        'Epsilon-Squared': None,
        'Note': note
      })

  # Create and display results
  kw_results = pd.DataFrame(results)
  print("\nKruskal-Wallis Results for metrics vs. point_outcome:")
  print(kw_results.sort_values(by='P-Value', ascending=True))

  # Step 5: Point-Biserial Correlation for 'FBK' vs. others
  if 'FBE' in ppr_df_clean['point_outcome'].values:
    ppr_df_clean['is_FBK'] = (ppr_df_clean['point_outcome'] == 'FBK').astype(int)
    pb_results = []
    for col in desired_cols:
      valid_data = ppr_df_clean[[col, 'is_FBK']].dropna()
      if len(valid_data) > 1 and len(valid_data['is_FBK'].unique()) > 1:
        corr, p_value = stats.pointbiserialr(valid_data['is_FBK'], valid_data[col])
        pb_results.append({'Metric': col, 'Correlation': corr, 'P-Value': p_value, 'Note': ''})
      else:
        pb_results.append({
          'Metric': col,
          'Correlation': None,
          'P-Value': None,
          'Note': 'Insufficient data or single category'
        })

    pb_results_df = pd.DataFrame(pb_results)
    print("\nPoint-Biserial Correlation for 'FBK' vs. others:")
    print(pb_results_df.sort_values(by='P-Value', ascending=True))

  # Step 6: Mutual Information (if scikit-learn is available)
  le = LabelEncoder()
  y_encoded = le.fit_transform(ppr_df_clean['point_outcome'])
  # Impute NaN with median for mutual information
  mi_data = ppr_df_clean[numerical_cols].copy()
  for col in numerical_cols:
    if not mi_data[col].isna().all():
      mi_data[col] = mi_data[col].fillna(mi_data[col].median())
    else:
      mi_data[col] = mi_data[col].fillna(0)  # Fallback for all-NaN columns
  mi_scores = mutual_info_classif(mi_data, y_encoded, random_state=42)
  mi_results = pd.DataFrame({
    'Metric': numerical_cols,
    'Mutual Information': mi_scores,
    'Note': ''
  })
  print("\nMutual Information Scores:")
  print(mi_results.sort_values(by='Mutual Information', ascending=False))
  # store thus to display
  mi_results = mi_results.sort_values(by='Mutual Information', ascending=False)
  return_results = mi_results.head(20)
  df_list[3] = return_results.to_dict('records')


  # Step 7: Visualize significant metrics (box plots)
  plt_num = 4
  significant_metrics = kw_results[kw_results['P-Value'] < 0.05]['Metric']
  for metric in significant_metrics:
    plt.figure(figsize=(8, 6))
    sns.boxplot(x='point_outcome', y=metric, data=ppr_df_clean)
    plt.title(f'Box Plot of {metric} by point_outcome')
    plt.show()
    if plt_num < 10:
      image_list[plt_num] = anvil.mpl_util.plot_image()
    plt_num = plt_num + 1
  '''
  
  
  return title_list, label_list, image_list, df_list

'''
@anvil.server.callable
def player_att_tendencies(lgy, team, **rpt_filters):
  
  Report Functions:
    - display attack tendencies by angule, we use the anguler zone for this

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Retrun Parameters:
    - title_list : a list of up to 10 titles to display on the report.  These all map to elements int he report_list data table
    - label_list : a list of up to 10 labels to display on the report, also coming from the report list data table 
    - image_list : a list of up to 10 imiages to plot data on the report
    - df_list : a list of up to 10 data frames to display talbles.  These are then converted to mkdn in the client
    
  

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


  ############### Third Populate the dataframe, assuming we have data returned

  # count zone 1 and 2 attacks

  ppr_df = ppr_df[ (ppr_df['att_yn'] == 'Y') & (ppr_df['att_player'] == disp_player )]
  att12 = ppr_df[ ( (ppr_df['att_src_zone_net'] == 1) | 
                    (ppr_df['att_src_zone_net'] == 2) ) &
                  (ppr_df['tactic'] != 'behind') ].shape[0]
  att45 = ppr_df[ ( (ppr_df['att_src_zone_net'] == 4) | 
                  (ppr_df['att_src_zone_net'] == 5) ) &
                  (ppr_df['tactic'] != 'behind') ].shape[0]

  
  att_front = '12' if att12 >= att45 else '45'
  att_posn = ['front','behind','middle']
  #angles = ['A1','A2','A3','A4','A5']
  print(f" attacks, from 1 and 2: {att12}, from 4 & 5: {att45}, att_front is {att_front}")
  
  for att in att_posn:
    new_df = ppr_df
    if att_front == '12' and att == 'front':
      angular_att_table1 = get_player_angular_attack_table(ppr_df[ ((ppr_df['att_src_zone_net'] == 1) ) & (ppr_df['tactic'] != 'behind')], player_data_stats_df, disp_player)
      angular_att_table2 = get_player_angular_attack_table(ppr_df[ ((ppr_df['att_src_zone_net'] == 2) ) & (ppr_df['tactic'] != 'behind')], player_data_stats_df, disp_player)
      df_list[0] = angular_att_table1.to_dict('records')
      df_list[1] = angular_att_table2.to_dict('records')
      plt_image1 = plot_volleyball_attacks( ppr_df[ ((ppr_df['att_src_zone_net'] == 1) ) & (ppr_df['tactic'] != 'behind')],'Pin Attacks in Front')
      plt_image2 = plot_volleyball_attacks( ppr_df[ ((ppr_df['att_src_zone_net'] == 2) ) & (ppr_df['tactic'] != 'behind')], 'Slot Attacks in Front')
      image_list[0] = plt_image1
      image_list[1] = plt_image2
    elif att_front == '12' and att == 'behind':
      angular_att_table = get_player_angular_attack_table(ppr_df[ (ppr_df['tactic'] == 'behind') ], player_data_stats_df, disp_player)
      df_list[2] = angular_att_table.to_dict('records')
      plt_image = plot_volleyball_attacks(ppr_df[ (ppr_df['tactic'] == 'behind') ],'Attacks Behind Setter')
      image_list[2] = plt_image
    elif att_front == '12' and att == 'middle':
      new_df = new_df[ ( (new_df['att_src_zone_net'] == 3) | (new_df['att_src_zone_net'] == 4) | (new_df['att_src_zone_net'] == 5) ) & (new_df['tactic'] != 'behind')]  
      angular_att_table = get_player_angular_attack_table(new_df, player_data_stats_df, disp_player)
      df_list[3] = angular_att_table.to_dict('records')
      plt_image = plot_volleyball_attacks(new_df,'Attacks from the Middle')
      image_list[3] = plt_image
      
    elif att_front == '45' and att == 'front':
      angular_att_table1 = get_player_angular_attack_table(ppr_df[ ((ppr_df['att_src_zone_net'] == 4) ) & (ppr_df['tactic'] != 'behind')], player_data_stats_df, disp_player)
      angular_att_table2 = get_player_angular_attack_table(ppr_df[ ((ppr_df['att_src_zone_net'] == 5) ) & (ppr_df['tactic'] != 'behind')], player_data_stats_df, disp_player)
      df_list[0] = angular_att_table1.to_dict('records')
      df_list[1] = angular_att_table2.to_dict('records')
      plt_image1 = plot_volleyball_attacks( ppr_df[ ((ppr_df['att_src_zone_net'] == 5) ) & (ppr_df['tactic'] != 'behind')],'Pin Attacks in Front')
      plt_image2 = plot_volleyball_attacks( ppr_df[ ((ppr_df['att_src_zone_net'] == 4) ) & (ppr_df['tactic'] != 'behind')],'Slot Attacks in Front')
      image_list[0] = plt_image1
      image_list[1] = plt_image2
    elif att_front == '45' and att == 'behind':
      angular_att_table = get_player_angular_attack_table(ppr_df[ (ppr_df['tactic'] == 'behind') ] , player_data_stats_df, disp_player)
      df_list[2] = angular_att_table.to_dict('records')
      plt_image = plot_volleyball_attacks(ppr_df[ (ppr_df['tactic'] == 'behind') ],'Attacks Behind Setter')
      image_list[2] = plt_image
    elif att_front == '45' and att == 'middle':
      new_df = new_df[ ( (new_df['att_src_zone_net'] == 3) | (new_df['att_src_zone_net'] == 1) | (new_df['att_src_zone_net'] == 2) ) & (new_df['tactic'] != 'behind')]   
      angular_att_table = get_player_angular_attack_table(new_df, player_data_stats_df, disp_player)
      df_list[3] = angular_att_table.to_dict('records')
      plt_image = plot_volleyball_attacks(new_df,'Attacks from the Middle')
      image_list[3] = plt_image


  return title_list, label_list, image_list, df_list




def get_player_angular_attack_table(new_df, player_data_stats_df, disp_player):
  # Define the structure of the DataFrame
  df_dict = {
    ' ': ['FBHE', 'FBSO', 'Kills', 'Errors', 'Attempts', '% of Attempts','% In System', 'URL'],
    'Cut-Left': [0, 0, 0, 0, 0, 0, 0, ' '],  # Zone A1
    'Angle-Left': [0, 0, 0, 0, 0, 0, 0, ' '],  # Zone A2
    'Over-Middle': [0, 0, 0, 0, 0, 0, 0, ' '],  # Zone A3
    'Angle-Right': [0, 0, 0, 0, 0, 0, 0, ' '],   # Zone A4
    'Cut-Right': [0, 0, 0, 0, 0, 0, 0, ' ']      # Zone A5
  }

  # Create DataFrame without setting an index
  angle_table = pd.DataFrame.from_dict(df_dict)

  print(f"get player angular attack table: df passed in: {new_df.shape[0]}, player: {disp_player}")
  print(f"angle table (initial):\n{angle_table}")

  angles = ['A1', 'A2', 'A3', 'A4', 'A5']
  ang_labels = ['Cut-Left', 'Angle-Left', 'Over-Middle', 'Angle-Right', 'Cut-Right']
  attempts = 0
  
  for i in range(5):
    # Filter the DataFrame for the current angular zone
    tmp_df = new_df[new_df['att_angular_zone'] == angles[i]]
    print(f"in Loop for i:{i}, ang_label: {ang_labels[i]}, angles: {angles[i]}, # of rows: {tmp_df.shape[0]}")

    # Compute metrics
    fbhe_vector = fbhe(tmp_df, disp_player, 'att', False)
    oos_vector = count_out_of_system(tmp_df, disp_player, 'att')

    # Update the DataFrame using row index (integer) and column (ang_labels[i])
    angle_table.loc[angle_table[' '] == 'FBHE', ang_labels[i]] = fbhe_vector[0]
    angle_table.loc[angle_table[' '] == 'FBSO', ang_labels[i]] = fbhe_vector[4]
    angle_table.loc[angle_table[' '] == 'Kills', ang_labels[i]] = fbhe_vector[1]
    angle_table.loc[angle_table[' '] == 'Errors', ang_labels[i]] = fbhe_vector[2]
    angle_table.loc[angle_table[' '] == 'Attempts', ang_labels[i]] = fbhe_vector[3]
    #angle_table.loc[angle_table[' '] == '% In System', ang_labels[i]] = 1 - oos_vector[1]  # Keep as float  
    # Optionally format as percentage for display later
    angle_table.loc[angle_table[' '] == '% In System', ang_labels[i]] = f"{(1 - oos_vector[1]):.1%}"
    attempts = fbhe_vector[3] + attempts
    print(f"Attempts: {attempts}")

  # calcualte percent of attempts
  for i in range(5):
    value = angle_table.loc[4, ang_labels[i]]
    print(f"Value: {value}")
    value = float(value)
    if attempts != 0:
      angle_table.loc[angle_table[' '] == '% of Attempts', ang_labels[i]] = str('{:.0%}').format( value/attempts )
    else:
      angle_table.loc[angle_table[' '] == '% of Attempts', ang_labels[i]] = 0
    print(f" number of attempts: { value }, attempts: {attempts}, percent of attempts: { angle_table.loc[angle_table[' '] == '% of Attempts', ang_labels[i]]}")
    #angle_table.loc[angle_table[' '] == '% of Attempts', ang_labels[i]] = str('{:.1%}').format(angle_table.loc[angle_table[' '] == '% of Attempts', ang_labels[i]])
    
  print(f"angular table (formatted for display):\n{angle_table}")

  return angle_table




def plot_volleyball_attacks(ppr_df, plt_title=''):
  # Create figure and axis
  fig, ax = plt.subplots(figsize=(10, 18))

  # Plot court background (assuming plot_court_background is defined)
  plot_court_background(fig, ax)

  # Define colors for outcomes
  outcome_colors = {
    'FBK': 'green',
    'FBE': 'red'
  }

  # Plot lines with arrows
  for idx, row in ppr_df.iterrows():
    src_x = row['att_src_x']
    src_y = row['att_src_y']
    dest_x = row['att_dest_x']
    dest_y = row['att_dest_y']
    outcome = row['point_outcome']

    # Set color based on outcome
    color = outcome_colors.get(outcome, 'blue')

    # Plot line
    ax.plot([src_x, dest_x], [src_y, dest_y], color=color, alpha=0.9)

    # Calculate direction vector for the arrow
    dx = dest_x - src_x
    dy = dest_y - src_y
    # Normalize the direction vector to a small length for the arrow
    length = np.sqrt(dx**2 + dy**2)
    if length > 0:  # Avoid division by zero
      dx = dx / length * 0.5  # Scale for arrow size
      dy = dy / length * 0.5
      # Add arrow at destination, aligned with line direction
      ax.arrow(dest_x - dx, dest_y - dy, dx, dy, 
               head_width=0.2, head_length=0.3, 
               fc=color, ec=color, alpha=0.9)

    # Process ellipses for each angular zone
  angular_zones = ['A1', 'A2', 'A3', 'A4', 'A5']

  for zone in angular_zones:
    # Filter data for the current angular zone
    zone_df = ppr_df[ppr_df['att_angular_zone'] == zone]
    if zone_df.empty:
      continue

      # Second standard deviation ellipse (all points in zone)
    dest_points = zone_df[['att_dest_x', 'att_dest_y']].values
    if len(dest_points) > 1:
      mean = np.mean(dest_points, axis=0)
      cov = np.cov(dest_points, rowvar=False)
      # Chi-square for 2 std devs (~95% confidence)
      lambda_, v = np.linalg.eigh(cov)
      lambda_ = np.sqrt(lambda_) * np.sqrt(chi2.ppf(0.68, df=2))
      ellipse = Ellipse(xy=mean, width=lambda_[0]*2, height=lambda_[1]*2, 
                        angle=np.degrees(np.arctan2(*v[:,0][::-1])),
                        edgecolor='orange', fc='orange', alpha=0.3)
      ax.add_patch(ellipse)

      # First standard deviation ellipse (kills only)
    kill_df = zone_df[zone_df['point_outcome'] == 'FBK']
    if len(kill_df) > 1:
      kill_points = kill_df[['att_dest_x', 'att_dest_y']].values
      mean = np.mean(kill_points, axis=0)
      cov = np.cov(kill_points, rowvar=False)
      # Chi-square for 1 std dev (~68% confidence)
      lambda_, v = np.linalg.eigh(cov)
      lambda_ = np.sqrt(lambda_) * np.sqrt(chi2.ppf(0.50, df=2))
      ellipse = Ellipse(xy=mean, width=lambda_[0]*2, height=lambda_[1]*2, 
                        angle=np.degrees(np.arctan2(*v[:,0][::-1])),
                        edgecolor='lightgreen', fc='lightgreen', alpha=0.5)
      ax.add_patch(ellipse)

    # Set plot limits and aspect ratio
  ax.set_xlim(-1, 9)  # Assuming standard volleyball court dimensions
  ax.set_ylim(-9, 9)
  #ax.set_aspect('equal')
  ax.set_title(plt_title, fontsize=35)


  # Create Anvil plot component

  plt_image = anvil.mpl_util.plot_image()
  return plt_image
'''

def player_correlation_set(lgy, team, **rpt_filters):
  '''
    Report Functions:
        - compute correlations between point difference (disp_pair's points minus opponent's points) and actual metrics (e.g., fbhe_noace_a, fbhe_noace_b, fbso_noace_a, fbso_noace_b) for the specified pair (team)
        - generate a horizontal bar plot for all correlations (image_list[0], sorted with largest correlation at top) and a 4x2 subplot for top/bottom 4 correlated metrics (image_list[1], with point_diff on x-axis) using Matplotlib
        - sort correlation DataFrame by coefficient (highest to lowest) before returning
        - debug metric availability and handle cases where metrics are missing
        - rename fbhe and fbso columns (e.g., fbhe_a_noace to fbhe_noace_a) and treat them as separate metrics

    INPUT Parameters:
        - lgy : league, gender, year combination (as in dropdowns)
        - team : the team of the user calling the report
        - rpt_filters : the list of filters to limit the data, including 'player', 'pair', 'start_date', 'end_date'

    OUTPUT Cognizance: The player_correlation_set function renames fbhe and fbso columns (e.g., fbhe_a_noace to fbhe_noace_a), treats fbhe_noace, fbhe_withace, fbso_noace, and fbso_withace as separate metrics, and calculates correlations between the point difference (disp_pair's points minus opponent's points) and actual metrics for a specified team (disp_pair from rpt_filters['pair']). It generates two plots using Matplotlib: a horizontal bar plot for all correlations (image_list[0], sorted with largest correlation at top) and a 4x2 subplot of scatter plots for the top 4 and bottom 4 correlated metrics (image_list[1], with point_diff on x-axis), saved using anvil.mpl_util.plot_image. The correlation DataFrame is sorted by coefficient (highest to lowest). If no metrics are available, an error is returned in df_list.

    OUTPUT Return Parameters:
        - title_list : a list of up to 10 titles to display on the report
        - label_list : a list of up to 10 labels to display on the report
        - image_list : a list of up to 10 images to plot data on the report
        - df_list : a list of up to 10 data frames to display tables
    '''
  #------------------------------------------------------------------------------------------------------
  # Initialize all lists, get and filter the data, and fetch information from report_list
  #-----------------------------------------------------------------------------------------------------
  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch player data (though not used in correlation analysis, kept for consistency)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)

  # Initiate return lists
  title_list = [''] * 10
  label_list = [''] * 10
  image_list = [''] * 10
  df_list = [''] * 10

  # Fetch the labels from the database
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
  # Correlation Analysis
  #-----------------------------------------------------------------------------------------------------
  # Fetch tri data
  disp_start_date = rpt_filters.get('start_date')
  disp_end_date = rpt_filters.get('end_date')
  tri_df, tri_df_found = get_tri_data(disp_league, disp_gender, disp_year, False, disp_start_date, disp_end_date)

  if not tri_df_found or tri_df.empty:
    df_list[0] = pd.DataFrame({'Error': ['No data found for the specified filters']}).to_dict('records')
    image_list[0] = ''
    image_list[1] = ''
    return title_list, label_list, image_list, df_list

    # Get disp_pair from rpt_filters
  disp_pair = rpt_filters.get('pair')
  if not disp_pair:
    df_list[0] = pd.DataFrame({'Error': ['No pair specified in rpt_filters']}).to_dict('records')
    image_list[0] = ''
    image_list[1] = ''
    return title_list, label_list, image_list, df_list

    # Rename fbhe and fbso columns (e.g., fbhe_a_noace -> fbhe_noace_a)
  rename_dict = {
    'fbhe_a_noace': 'fbhe_noace_a',
    'fbhe_b_noace': 'fbhe_noace_b',
    'fbhe_a_withace': 'fbhe_withace_a',
    'fbhe_b_withace': 'fbhe_withace_b',
    'fbso_a_noace': 'fbso_noace_a',
    'fbso_b_noace': 'fbso_noace_b',
    'fbso_a_withace': 'fbso_withace_a',
    'fbso_b_withace': 'fbso_withace_b'
  }
  tri_df = tri_df.rename(columns=rename_dict)

  # Debug: Print columns after renaming
  print("Columns in tri_df after renaming:", tri_df.columns.tolist())

  # Filter tri_df to rows where disp_pair is in teama or teamb
  tri_df_filtered = tri_df[tri_df['teama'] == disp_pair].combine_first(tri_df[tri_df['teamb'] == disp_pair])

  if tri_df_filtered.empty:
    df_list[0] = pd.DataFrame({'Error': [f"No data found for pair {disp_pair} in teama or teamb"]}).to_dict('records')
    image_list[0] = ''
    image_list[1] = ''
    return title_list, label_list, image_list, df_list

    # Debug: Print columns in tri_df_filtered
  print("Columns in tri_df_filtered:", tri_df_filtered.columns.tolist())

  # Calculate point difference (disp_pair's points minus opponent's points)
  df_adjusted = tri_df_filtered.copy()
  df_adjusted['is_teama'] = df_adjusted['teama'] == disp_pair
  df_adjusted['point_diff'] = df_adjusted.apply(
    lambda row: row['teama_pts'] - row['teamb_pts'] if row['is_teama'] else row['teamb_pts'] - row['teama_pts'],
    axis=1
  )

  # Define actual metrics based on tri_df columns (using _a and _b suffixes)
  actual_metrics = [
    'fbhe_noace', 'fbhe_withace', 'fbso_noace', 'fbso_withace',
    'eso', 'ace_error', 't_eff', 'knockout', 'goodpass',
    'tcr', 'err_den'
  ]

  # Check available metrics in tri_df_filtered
  available_metrics = []
  for metric in actual_metrics:
    a_col = f'{metric}_a'
    b_col = f'{metric}_b'
    if a_col in tri_df_filtered.columns and b_col in tri_df_filtered.columns:
      available_metrics.append(metric)

    # Debug: Print available metrics
  print("Available metrics:", available_metrics)

  if not available_metrics:
    df_list[0] = pd.DataFrame({'Error': ['No valid metrics found in tri_df. Expected columns like fbhe_noace_a, fbhe_noace_b, etc.']}).to_dict('records')
    image_list[0] = ''
    image_list[1] = ''
    return title_list, label_list, image_list, df_list

    # Create a new DataFrame with metrics for disp_pair
  for metric in available_metrics:
    a_col = f'{metric}_a'
    b_col = f'{metric}_b'
    df_adjusted[metric] = df_adjusted.apply(
      lambda row: row[a_col] if row['is_teama'] else row[b_col], axis=1
    )

    # Debug: Print columns in df_adjusted after metric selection
  print("Columns in df_adjusted:", df_adjusted.columns.tolist())

  # Adjust metrics (but not point_diff) based on whether disp_pair is the winner
  for index, row in df_adjusted.iterrows():
    is_winner = (row['winning_team'] == disp_pair)
    if not is_winner:
      for metric in available_metrics:
        if metric in df_adjusted.columns and pd.notnull(row[metric]):
          df_adjusted.at[index, metric] = -row[metric]

    # Calculate correlations
  correlations = {}
  for metric in available_metrics:
    if metric in df_adjusted.columns:
      valid_data = df_adjusted[['point_diff', metric]].dropna()
      if not valid_data.empty:
        correlations[metric] = valid_data['point_diff'].corr(valid_data[metric])

    # Debug: Print correlations
  print("Calculated correlations:", correlations)

  # Create correlation DataFrame and sort by Correlation (highest to lowest)
  if not correlations:
    df_list[0] = pd.DataFrame({'Error': ['No correlations could be calculated. Check data for null values or insufficient rows.']}).to_dict('records')
    image_list[0] = ''
    image_list[1] = ''
    return title_list, label_list, image_list, df_list

  corr_df = pd.DataFrame.from_dict(correlations, orient='index', columns=['Correlation'])
  corr_df = corr_df.reset_index().rename(columns={'index': 'Metric'})
  corr_df = corr_df.sort_values(by='Correlation', ascending=False)  # Ensures largest correlation at top
  df_list[0] = corr_df.to_dict('records')
    
  #------------------------------------------------------------------------------------------------------
  # Generate Plots with Matplotlib
  #-----------------------------------------------------------------------------------------------------
  if not corr_df.empty:
    # 1. Horizontal Bar Plot for All Correlations (image_list[0], largest correlation at top)
    plt.figure(figsize=(10, 8))
    colors = ['red' if x < 0 else 'blue' for x in corr_df['Correlation']]
    plt.barh(corr_df['Metric'], corr_df['Correlation'], color=colors)
    plt.xlabel('Correlation Coefficient')
    plt.ylabel('Metric')
    plt.title(f'Correlation of Metrics with Point Difference for {disp_pair}\n({disp_league}, {disp_gender}, {disp_year})')
    plt.xlim(-1, 1)
    plt.grid(True, axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    image_list[0] = anvil.mpl_util.plot_image()
    plt.close()
        
    # 2. 4x2 Subplot for Top 4 and Bottom 4 Correlated Metrics (image_list[1], point_diff on x-axis)
    corr_df_sorted = corr_df.sort_values(by='Correlation', key=abs, ascending=False)
    top_bottom_metrics = corr_df_sorted['Metric'].head(8).tolist()
    if len(top_bottom_metrics) > 4:
      top_metrics = top_bottom_metrics[:4]
      bottom_metrics = top_bottom_metrics[-4:][::-1]  # Reverse for readability
    else:
      top_metrics = top_bottom_metrics
      bottom_metrics = []
        
    fig, axes = plt.subplots(4, 2, figsize=(10, 8), sharex=True, sharey=True)
    axes = axes.flatten()
    for i, metric in enumerate(top_metrics + bottom_metrics):
      valid_data = df_adjusted[['point_diff', metric]].dropna()
      if not valid_data.empty:
        axes[i].scatter(valid_data['point_diff'], valid_data[metric], alpha=0.6)
        axes[i].set_title(metric, fontsize=10)
        axes[i].grid(True, linestyle='--', alpha=0.7)
        if i % 2 == 0:
          axes[i].set_ylabel('Metric Value')
        if i >= 6:
          axes[i].set_xlabel('Point Difference')
        
    plt.suptitle(f'Top and Bottom Correlated Metrics with Point Difference for {disp_pair}\n({disp_league}, {disp_gender}, {disp_year})', fontsize=12)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    image_list[1] = anvil.mpl_util.plot_image()
    plt.close()
    
    return title_list, label_list, image_list, df_list


#---------------------------------------------------------------------------
#
#              player report player 
#
#---------------------------------------------------------------------------
def report_player_sets(lgy, team, **rpt_filters):
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

  # For FBHE, filter to attacks by the player
  ppr_df = ppr_df[ppr_df['att_player'] == disp_player]

  # Bin the set_distance and set_height into 0.5 meter increments
  distance_bins = np.arange(0, 10.5, 0.5)
  height_bins = np.arange(0, 6.5, 0.5)
  ppr_df['distance_bin'] = pd.cut(ppr_df['set_dist'], bins=distance_bins, labels=(distance_bins[:-1] + distance_bins[1:]) / 2)
  ppr_df['height_bin'] = pd.cut(ppr_df['set_height'], bins=height_bins, labels=(height_bins[:-1] + height_bins[1:]) / 2)

  # Define function to calculate FBHE and attempts using fbhe_obj
  def calculate_fbhe(group):
    if group.empty:
      return pd.Series({'attempts': 0, 'fbhe': 0.0})
    result = fbhe_obj(group, disp_player, 'att', True)
    return pd.Series({'attempts': result.attempts, 'fbhe': result.fbhe,'URL':result.video_link})

  # Group by bins and apply the calculation
  grouped = ppr_df.groupby(['distance_bin', 'height_bin'], as_index=False).apply(calculate_fbhe)
  grouped = grouped[grouped['attempts'] > 4]

  # Table of attempts for df_list[0]
  attempts_table = grouped[['distance_bin', 'height_bin', 'attempts','fbhe','URL']].rename(columns={'distance_bin': 'set_dist', 'height_bin': 'set_height'})
  df_list[0] = attempts_table.to_dict('records')

  ## Table of fbhe for df_list[1]
  #fbhe_table = grouped[['distance_bin', 'height_bin', 'fbhe']].rename(columns={'distance_bin': 'set_dist', 'height_bin': 'set_height'})
  #df_list[1] = fbhe_table.to_dict('records')
  
  # Get mean and stdev for color scaling
  fbhe_mean = player_data_stats_df['fbhe_mean']
  fbhe_stdev = player_data_stats_df['fbhe_stdev']
  vmin = fbhe_mean - fbhe_stdev
  vmax = fbhe_mean + fbhe_stdev
  xmin = 0
  xmax = 8
  ymin = 1
  ymax = 4
  attmin = 5
  attmax = 20

  # FBHE scatter plot
  fig1, ax1 = plt.subplots()
  scatter1 = ax1.scatter(grouped['distance_bin'], grouped['height_bin'], c=grouped['fbhe'], cmap='RdYlGn', vmin=vmin, vmax=vmax, s=80)
  fig1.colorbar(scatter1)
  ax1.set_xlim(xmin, xmax)
  ax1.set_ylim(ymin, ymax)
  ax1.set_xticks(np.arange(xmin, xmax+0.5, 0.5))
  ax1.set_yticks(np.arange(ymin, ymax+0.5, 0.5))
  ax1.set_xlabel('Set Distance (m)')
  ax1.set_ylabel('Set Height (m)')
  ax1.set_title(f'{disp_player} FBHE by Set Position')
  image_list[0] = anvil.mpl_util.plot_image()

  # Attempts scatter plot
  fig2, ax2 = plt.subplots()
  scatter2 = ax2.scatter(grouped['distance_bin'], grouped['height_bin'], c=grouped['attempts'], cmap='RdYlGn', vmin=attmin, vmax=attmax, s=80)
  fig2.colorbar(scatter2)
  ax2.set_xlim(xmin, xmax)
  ax2.set_ylim(ymin, ymax)
  ax2.set_xticks(np.arange(xmin, xmax+0.5, 0.5))
  ax2.set_yticks(np.arange(ymin, ymax+0.5, 0.5))
  ax2.set_xlabel('Set Distance (m)')
  ax2.set_ylabel('Set Height (m)')
  ax2.set_title(f'{disp_player} Attempts by Set Position (colored by Attempts)')
  image_list[1] = anvil.mpl_util.plot_image()

  #--------------------------------------------------------------------
  #
  #     Now, calculate the transition table
  #
  #--------------------------------------------------------------------
  '''

  # calcualte the transition obj routine, then store it in a table
  trans_dict = {
    ' ':['Percent','Percentile','Kills',"Opponent's Errors","Opponent's Kills",'Errors','Total Points'],
    'Transition Conversion Rate':[0,0,0,0,0,0],
    'Transition Efficiency':[0,0,0,0,0,0],
    'Transition Create':[0,0,0,0,0,0]
  }
  trans_table = pd.DataFrame.from_dict(trans_dict)


  trans_obj = calc_trans_obj(ppr_df, disp_player, 'rcv')
  print(f"Transition Object:{trans_obj}")
  print(f"tcr {trans_obj.get('tcr_str')}, t_eff : {trans_obj.get('t_eff_str')}, t_create : {trans_obj.get('treate_str')}")
  trans_table.at['Percent','Transition Conversion Rate'] = trans_obj.get('tcr_str')
  trans_table.at['Percent','Transition Efficiency'] = trans_obj.get('t_eff_str')
  trans_table.at['Percent','Transition Create'] = trans_obj.get('t_create_str')
  trans_table.at['Percentile','Transition Conversion Rate'] = trans_obj.get('tcr_str')
  trans_table.at['Percentile','Transition Efficiency'] = trans_obj.get('t_eff_str')
  trans_table.at['Percentile','Transition Create'] = trans_obj.get('t_create_str')
  trans_table.at['Kills','Transition Conversion Rate'] = trans_obj.get('tran_kills_won')
  trans_table.at["Opponent's Errors",'Transition Conversion Rate'] = trans_obj.get('tran_errors_won')
  trans_table.at["Opponent's Kills",'Transition Conversion Rate'] = trans_obj.get('tran_kills_lost')
  trans_table.at['Errors','Transition Conversion Rate'] = trans_obj.get('tran_errors_lost')
  trans_table.at['Total Points','Transition Conversion Rate'] = trans_obj.get('tran_total_pts')

  df_list[1] = trans_table.to_dict('records')
  '''
  
  return title_list, label_list, image_list, df_list



@anvil.server.callable
def report_player_attack_transition(lgy, team, **rpt_filters):
  '''
  Report Function to display a table of player attack transition metrics.

  INPUT Parameters:
    - lgy : league, gender, year combination (as in dropdowns)
    - team : the team of the user calling the report
    - rpt_filters : the list of filters to limit the data

  OUTPUT Return Parameters:
    - title_list : a list of up to 10 titles to display on the report
    - label_list : a list of up to 10 labels to display on the report
    - image_list : a list of up to 10 images to plot data on the report
    - df_list : a list of up to 10 data frames to display tables
  '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch information from report_list
  #-----------------------------------------------------------------------------------------------------
  # lgy is the league+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe and filter by all the report filters
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)
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
  #            Create the dataframe for player attack transition metrics
  #-----------------------------------------------------------------------------------------------------
  disp_player = rpt_filters.get('player')
  # Filter ppr_df to only include rows where att_player matches disp_player
  ppr_df = ppr_df[ppr_df['att_player'].str.strip() == disp_player.strip()]

  # Initialize the table data
  table_data = {
    'Metric': [
      'Transition Conversion', 'Percentile',
      'Transition Effectiveness', 'Percentile',
      'Transition Creates', 'Percentile',
      'Transition Points'
    ],
    'All': ['', '', '', '', '', '', ''],
    'Area 1': ['', '', '', '', '', '', ''],
    'Area 2': ['', '', '', '', '', '', ''],
    'Area 3': ['', '', '', '', '', '', ''],
    'Area 4': ['', '', '', '', '', '', ''],
    'Area 5': ['', '', '', '', '', '', ''],
    'No Area': ['', '', '', '', '', '', '']
  }

  # Helper function to calculate percentile
  def calculate_percentile(metric, mean, std_dev):
    if std_dev == 0 or metric is None or mean is None or std_dev is None:
      return None, None
    z_score = (metric - mean) / std_dev
    percentile = stats.norm.cdf(z_score)
    percentile_str = f"{percentile * 100:.1f}%"
    return percentile, percentile_str

  # Calculate metrics for 'All'
  trans_obj_all = calc_trans_obj(ppr_df, disp_player, 'rcv')
  if trans_obj_all['status']:
    table_data['All'][0] = trans_obj_all['tcr_str']  # Transition Conversion
    _, table_data['All'][1] = calculate_percentile(trans_obj_all['tcr'], player_data_stats_df.at[0,'tcr_mean'], player_data_stats_df.at[0,'tcr_stdev'])  # Percentile
    table_data['All'][2] = trans_obj_all['t_eff_str']  # Transition Effectiveness
    _, table_data['All'][3] = calculate_percentile(trans_obj_all['t_eff'], player_data_stats_df.at[0,'t_eff_mean'], player_data_stats_df.at[0,'t_eff_stdev'])  # Percentile
    table_data['All'][4] = trans_obj_all['t_create_str']  # Transition Creates
    _, table_data['All'][5] = calculate_percentile(trans_obj_all['t_create'], player_data_stats_df.at[0,'t_create_mean'], player_data_stats_df.at[0,'t_create_stdev'])  # Percentile
    table_data['All'][6] = str(trans_obj_all['tran_total_pts'])  # Transition Points

  # Calculate metrics for each area (1 to 5)
  for area in range(1, 6):
    area_df = ppr_df[ppr_df['att_src_zone_net'] == area]
    trans_obj_area = calc_trans_obj(area_df, disp_player, 'rcv')
    if trans_obj_area['status']:
      table_data[f'Area {area}'][0] = trans_obj_area['tcr_str']
      _, table_data[f'Area {area}'][1] = calculate_percentile(trans_obj_area['tcr'], player_data_stats_df.at[0,'tcr_mean'], player_data_stats_df.at[0,'tcr_stdev'])
      table_data[f'Area {area}'][2] = trans_obj_area['t_eff_str']
      _, table_data[f'Area {area}'][3] = calculate_percentile(trans_obj_area['t_eff'], player_data_stats_df.at[0,'t_eff_mean'], player_data_stats_df.at[0,'t_eff_stdev'])
      table_data[f'Area {area}'][4] = trans_obj_area['t_create_str']
      _, table_data[f'Area {area}'][5] = calculate_percentile(trans_obj_area['t_create'], player_data_stats_df.at[0,'t_create_mean'], player_data_stats_df.at[0,'t_create_stdev'])
      table_data[f'Area {area}'][6] = str(trans_obj_area['tran_total_pts'])

  # Calculate metrics for 'No Area'
  no_area_df = ppr_df[ (ppr_df['att_src_zone_net'] != 1) & 
                       (ppr_df['att_src_zone_net'] != 2) & 
                       (ppr_df['att_src_zone_net'] != 3) & 
                       (ppr_df['att_src_zone_net'] != 4) & 
                       (ppr_df['att_src_zone_net'] != 5) 
                        ]
  trans_obj_no_area = calc_trans_obj(no_area_df, disp_player, 'rcv')
  if trans_obj_no_area['status']:
    table_data['No Area'][0] = trans_obj_no_area['tcr_str']
    _, table_data['No Area'][1] = calculate_percentile(trans_obj_no_area['tcr'], player_data_stats_df.at[0,'tcr_mean'], player_data_stats_df.at[0,'tcr_stdev'])
    table_data['No Area'][2] = trans_obj_no_area['t_eff_str']
    _, table_data['No Area'][3] = calculate_percentile(trans_obj_no_area['t_eff'], player_data_stats_df.at[0,'t_eff_mean'], player_data_stats_df.at[0,'t_eff_stdev'])
    table_data['No Area'][4] = trans_obj_no_area['t_create_str']
    _, table_data['No Area'][5] = calculate_percentile(trans_obj_no_area['t_create'], player_data_stats_df.at[0,'t_create_mean'], player_data_stats_df.at[0,'t_create_stdev'])
    table_data['No Area'][6] = str(trans_obj_no_area['tran_total_pts'])

  # Convert table_data to DataFrame
  df = pd.DataFrame(table_data)

  # Store the dataframe in df_list[0]
  df_list[0] = df.to_dict('records')

  return title_list, label_list, image_list, df_list



def report_player_expected_value(lgy, team, **rpt_filters):
  '''
    Report Function to display a table of player metrics by area.

    INPUT Parameters:
      - lgy : league, gender, year combination (as in dropdowns)
      - team : the team of the user calling the report
      - rpt_filters : the list of filters to limit the data

    OUTPUT Return Parameters:
      - title_list : a list of up to 10 titles to display on the report
      - label_list : a list of up to 10 labels to display on the report
      - image_list : a list of up to 10 images to plot data on the report
      - df_list : a list of up to 10 data frames to display tables
    '''
  #------------------------------------------------------------------------------------------------------
  # Initialize all lists, get and filter the data, and fetch information from report_list
  #------------------------------------------------------------------------------------------------------
  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe and filter by all the report filters
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)

  # Initiate return lists
  title_list = [''] * 10
  label_list = [''] * 10
  image_list = [''] * 10
  df_list = [''] * 10

  # Fetch the labels from the database
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
  # Create the table with filtered ppr data for report
  #------------------------------------------------------------------------------------------------------
  # Limit the data to plays where att_player is disp_player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ppr_df['att_player'] == disp_player]

  # Initialize table data
  table_data = {
    'Metric': ['FBHE', 'Percentile', 'Transition Conversion', 'Percentile', 'Expected Value', 'Percentile', 'Transition Points'],
    'All': [0.0, 0.0, 0.0, 0.0, '0.00%', 0.0, 0.0],
    'Area 1': [0.0, 0.0, 0.0, 0.0, '0.00%', 0.0, 0.0],
    'Area 2': [0.0, 0.0, 0.0, 0.0, '0.00%', 0.0, 0.0],
    'Area 3': [0.0, 0.0, 0.0, 0.0, '0.00%', 0.0, 0.0],
    'Area 4': [0.0, 0.0, 0.0, 0.0, '0.00%', 0.0, 0.0],
    'Area 5': [0.0, 0.0, 0.0, 0.0, '0.00%', 0.0, 0.0],
    'No Area': [0.0, 0.0, 0.0, 0.0, '0.00%', 0.0, 0.0]
  }

  # Calculate metrics for each area
  areas = [1, 2, 3, 4, 5]
  for area in ['All'] + [f'Area {i}' for i in areas] + ['No Area']:
    if area == 'All':
      temp_df = ppr_df
    elif area == 'No Area':
      temp_df = ppr_df[~ppr_df['att_src_zone_net'].isin(areas)]
    else:
      area_num = int(area.split()[-1])
      temp_df = ppr_df[ppr_df['att_src_zone_net'] == area_num]

    # Calculate FBHE
    fbhe_result = fbhe_obj(temp_df, disp_player, play_type='att', video_yn=False)
    table_data[area][0] = fbhe_result.fbhe
    # Use percentile_str from calculate_percentile
    _, fbhe_percentile_str = calculate_percentile(
      table_data[area][0],
      player_data_stats_df.at[0, 'fbhe_mean'],
      player_data_stats_df.at[0, 'fbhe_stdev']
    )
    table_data[area][1] = fbhe_percentile_str if fbhe_percentile_str is not None else '0.00%'

    # Calculate Transition Conversion
    trans_result = calc_trans_obj(temp_df, disp_player, flag='rcv')
    table_data[area][2] = trans_result.get('tcr_str', 0.0)
    tcr_value = trans_result.get('tcr', 0.0)
    # Use percentile_str from calculate_percentile
    _, tcr_percentile_str = calculate_percentile(
      tcr_value,
      player_data_stats_df.at[0, 'tcr_mean'],
      player_data_stats_df.at[0, 'tcr_stdev']
      )
    table_data[area][3] = tcr_percentile_str if tcr_percentile_str is not None else '0%'

    # Calculate Expected Value
    ev_result = calc_ev_obj(temp_df, disp_player)
    table_data[area][4] = ev_result.get('expected_value', '0%')
    # Convert percentage string to float for percentile calculation
    ev_value = float(ev_result.get('expected_value', '0%').strip('%')) 
    #print(f" ev_value {ev_value}, mean {player_data_stats_df.at[0, 'expected_mean']}, stdev {player_data_stats_df.at[0, 'expected_stdev']}")
    _, ev_percentile_str = calculate_percentile(
      ev_value,
      player_data_stats_df.at[0, 'expected_mean'],
      player_data_stats_df.at[0, 'expected_stdev']
    )
    table_data[area][5] = ev_percentile_str if ev_percentile_str is not None else '0%'

    # Calculate Transition Points
    table_data[area][6] = trans_result.get('tran_total_pts', 0.0)

  # Convert to DataFrame
  df = pd.DataFrame(table_data)
  df_list[0] = df.to_dict('records')

  return title_list, label_list, image_list, df_list
