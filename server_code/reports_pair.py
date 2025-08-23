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


from tabulate import tabulate
from server_functions import *
from anvil import pdf
from pair_functions import *
from matchup_reports import player_45_serves
from plot_functions import *
from datetime import datetime, timedelta
from server_functions import *
from plot_functions import *

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

  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe and player stats
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  pair_data_df, pair_data_stats_df = get_pair_data(disp_league, disp_gender, disp_year)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

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

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


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
  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe and player stats
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
  #pair_data_df, pair_data_stats_df = get_pair_data(disp_league, disp_gender, disp_year)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)

  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)
  

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

    return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list
