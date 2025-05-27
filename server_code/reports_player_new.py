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
  title_list[5] = rpt_row['company_name']
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
#.  Player COnsistency Report
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
#.  Player COnsistency Report
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
    start_date = datetime(2025, 2, 15)
    
  if 'end_date' in rpt_filters:
    end_date = rpt_filters.get('end_date')
    num_weeks = (end_date - start_date)/7
  else:
    end_date = start_date + 7*num_weeks
    
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
  df_dict = {'Variable':['FBHE','Errors','Transition','Knockout','Good Pass','Points'],
               'Week 1':[0,0,0,0,0,0],
               'Week 2':[0,0,0,0,0,0],
               'Week 3':[0,0,0,0,0,0],
               'Week 4':[0,0,0,0,0,0],
               'Week 5':[0,0,0,0,0,0],
               'Week 6':[0,0,0,0,0,0],
               'Week 7':[0,0,0,0,0,0],
               'Week 8':[0,0,0,0,0,0],
               'Week 9':[0,0,0,0,0,0],
               'Week 10':[0,0,0,0,0,0],
               'Week 11':[0,0,0,0,0,0]
    }
  sum_df = pd.DataFrame.from_dict(df_dict)
    
  # start a loop over the weeks
  for i in range(1,num_weeks):
    # filter ppr_df to tmp_df for this week
    tmp_df = ppr_df[ (ppr_df['date'] >= weekly_dates[i]['start_date']) & (ppr_df['date'] < weekly_date[i]['end_date']) ]
    pt_totals_df = player_pt_total( tmp_df, disp_player )
    sum_df.at['week1','FBHE'] = (pt_totals_df.at[0,'p_fbk']-pt_totals_df.at[0,'p_fbe'])/( pt_totals_df.at[0,'p_att_total'])
    sum_df.at['week1','Errors'] = (pt_totals_df.at[0,'p_fbe']+pt_totals_df.at[0,'p_tse']+pt_totals_df.at[0,'p_te_r']+pt_totals_df.at[0,'p_te_s'])/( pt_totals_df.at[0,'pts_total'])
    sum_df.at['week1','Transition'] = (pt_totals_df.at[0,'p_tk_s']+pt_totals_df.at[0,'p_tk_r']+pt_totals_df.at[0,'o_te_r']+pt_totals_df.at[0,'o_te_s'])/( pt_totals_df.at[0,'pts_total'])
    sum_df.at['week1','Knockout'] = (pt_totals_df.at[0,'p_tsa']+pt_totals_df.at[0,'o_bad_pass'])/( pt_totals_df.at[0,'pts_total'])
    sum_df.at['week1','Good Pass'] = (pt_totals_df.at[0,'p_good_pass'])/( pt_totals_df.at[0,'p_good_pass']+pt_totals_df.at[0,'p_bad_pass'])
    sum_df.at['week1','Points'] = ( (pt_totals_df.at[0,'p_tsa']+pt_totals_df.at[0,'p_fbk']+pt_totals_df.at[0,'p_tk_r']+pt_totals_df.at[0,'p_tk_s']) +
                                      (pt_totals_df.at[0,'0_tse']+pt_totals_df.at[0,'o_fbe']+pt_totals_df.at[0,'o_te_r']+pt_totals_df.at[0,'o_te_s']) ) / ( pt_totals_df.at[0,'pts_total']
                                      )

  print(f" Summary dataframe: {sum_df}")
  
  # now create histograms for each one
  plt1 = create_histogram( sum_df['FBHE'], sum_df['Variable'], 'FBHE', 'FBHE' )
    
  
  
  return title_list, label_list, image_list, df_list