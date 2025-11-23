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
from anvil import pdf

# import error logging funcitons
from logger_utils import log_info, log_error, log_critical, log_debug

from confidence_intervals import wilson_score_ci

# Import other modules
from pair_functions import *
from plot_functions import *
from datetime import datetime, timedelta
from server_functions import *
from plot_functions import *
#from confidence_intervals import add_ci_to_fbhe_result



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

  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # fetch the ppr dataframe and filter by all the report filters
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, 'League', True)
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
  high = (player_data_stats_df.at[0,'tcr_mean']+2*player_data_stats_df.at[0,'tcr_stdev'])
  low = (player_data_stats_df.at[0,'tcr_mean']-2*player_data_stats_df.at[0,'tcr_stdev'])
  bar_width = (high-low)/30
  plot_df, status = count_wins( tri_df['win_tcr'], tri_df['loser_tcr'], low, high )
  print(f"high:{high}, low:{low}, plot_df \n {plot_df}")
  bar_plot = plot_bar_graph( plot_df['MidPoint'], plot_df['Win Percent'], 'Winning Percent by Transition Conversion Rate', 'Transition Conversion Rate', 'Percent Wins', plt_size, '', 0, True, player_data_stats_df.at[0,'tcr_mean'], player_data_stats_df.at[0,'tcr_stdev'], bar_width)
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

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


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
#              report to calculate and show the corelations in a league, to wining sets
#
#---------------------------------------------------------------------------
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
  # unpack lgy into league, gender, year
  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)
  
  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  
  # Fetch the ppr dataframe, and/or player stats, and/or tri-data
  # comment some in our out based on this reports needs.
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, 'League', True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # Filter the ppr dataframe
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # for this league report, we want to filter the tri_df by the rpt_filters, specifically comp_l1, and comp_l2, comp_l3, and game_date
  tri_df = filter_ppr_df( tri_df, **rpt_filters) # note that since filter_ppr_df tests if each column exitst first, this will work when passing hte tri_df

  # for this report, only need the triangle scoring table
  ppr_save = ppr_df

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

  # for this league report, we want to filter the tri_df by the rpt_filters, specifically comp_l1, and comp_l2, comp_l3, and game_date
  tri_df = filter_ppr_df( tri_df, **rpt_filters) # note that since filter_ppr_df tests if each column exitst first, this will work when passing hte tri_df
  
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

  #print(f"numberi df size {numeric_df.shape[0]}\n{numeric_df}")
  # Calculate Pearson correlation of all numeric columns with 'point_diff'
  correlations = numeric_df.corrwith(numeric_df['point_diff'])

  # Sort correlations in descending order for better readability
  correlations = correlations.sort_values(ascending=True)

  correlations = correlations.drop('point_diff', errors='ignore')

  # Round the 'Correlation' and 'P-value' columns to 3 decimal places
  correlations = correlations.round(3)
  correlations = correlations.dropna()

  top_corr = pd.concat([correlations.head(15), correlations.tail(15)])
  #print(f" correlations size: {len(correlations)}\n {correlations}")

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

  #print(f" correlations size: {len(correlations)}\n {correlations}")

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
  #print("Updated point_outcome values FBK & FBE:")
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
  
  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list

"""
Confidence Interval Report for Beach Volleyball Analytics
Anvil.works Integration

This module provides a report function to calculate and display confidence intervals
for player performance metrics across different sample sizes.
"""



def report_confidence_intervals(lgy, team, **rpt_filters):
  '''
    Report Functions:
        - Calculate and display confidence intervals for player metrics
        - Show how reliability changes with sample size
        - Compare actual player data with theoretical margins of error
    
    INPUT Parameters:
        - lgy : league, gender, year combination (as in dropdowns)
        - team : the team of the user calling the report
        - rpt_filters : the list of filters to limit the data
            - player : specific player to analyze
            - comp_l1, comp_l2, comp_l3 : competition filters
            - start_date, end_date : date range filters
    
    OUTPUT Return Parameters:
        - title_list : a list of up to 10 titles to display on the report
        - label_list : a list of up to 10 labels to display on the report
        - image_list : a list of up to 10 images to plot data on the report
        - df_list : a list of up to 10 data frames to display tables
        - df_desc_list : descriptions for each dataframe
        - image_desc_list : descriptions for each image
    '''

  #------------------------------------------------------------------------------------------------------
  #            Initialize all lists, get and filter the data, and fetch information from report_list
  #------------------------------------------------------------------------------------------------------

  # Initialize the calculated lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']
  df_desc_list = ['','','','','','','','','','']
  image_desc_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Get player from filters
  disp_player = rpt_filters.get('player')

  # Fetch the ppr dataframe and player stats
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)

  # Filter the ppr dataframe
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # Fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=inspect.currentframe().f_code.co_name)

  if rpt_row:
    title_list[0] = rpt_row['rpt_title']
    title_list[1] = rpt_row['rpt_sub_title']
    title_list[2] = rpt_row['rpt_section_title1']
    title_list[3] = rpt_filters.get('lgy')
    title_list[4] = rpt_row['team_name']
    title_list[5] = rpt_row['rpt_type']
    title_list[6] = rpt_row['filter_text']
    title_list[7] = rpt_row['explain_text']
    title_list[8] = disp_player if disp_player else 'All Players'
    title_list[9] = rpt_filters.get('pair', '')

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

    df_desc_list[0] = rpt_row['df_desc_1']
    df_desc_list[1] = rpt_row['df_desc_2']
    df_desc_list[2] = rpt_row['df_desc_3']
    df_desc_list[3] = rpt_row['df_desc_4']
    df_desc_list[4] = rpt_row['df_desc_5']
    df_desc_list[5] = rpt_row['df_desc_6']

    #------------------------------------------------------------------------------------------------------
    #            Create the images and dataframes with filtered ppr data for report
    #------------------------------------------------------------------------------------------------------

    # DF 0: Theoretical Margin of Error Reference Table
  df_list[0] = create_margin_of_error_table()

  # DF 1: Player's Actual Metrics with Confidence Intervals (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    df_list[1] = create_player_ci_metrics(ppr_df, disp_player, player_data_stats_df)

    # DF 2: Reliability Guidelines
  df_list[2] = create_reliability_guidelines()

  # DF 3: Sample Size Requirements for Target Precision
  df_list[3] = create_sample_size_requirements()

  # DF 4: Player Zone Analysis with CIs (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    df_list[4] = create_zone_confidence_analysis(ppr_df, disp_player, player_data_stats_df)

    # DF 5: How Metrics Change Over Season (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    df_list[5] = create_cumulative_ci_analysis(ppr_df, disp_player)

    # IMAGE 0: Margin of Error vs Sample Size Plot
  fig = plot_margin_vs_sample_size()
  image_list[0] = anvil.mpl_util.plot_image(fig)
  
  # IMAGE 1: Confidence Interval Visualization for Player (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    fig = plot_player_ci_visualization(ppr_df, disp_player)
    image_list[1] = anvil.mpl_util.plot_image(fig)

  # IMAGE 2: Zone Comparison with Error Bars (if player specified)
  if disp_player and ppr_df.shape[0] > 0:
    fig = plot_zone_comparison_with_ci(ppr_df, disp_player)
    image_list[2] = anvil.mpl_util.plot_image(fig)

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


#------------------------------------------------------------------------------------------------------
#                           Helper Functions for Confidence Interval Calculations
#------------------------------------------------------------------------------------------------------

def wilson_score_ci(successes, attempts, confidence=0.95):
  """Calculate Wilson Score Confidence Interval for a proportion."""
  if attempts == 0:
    return {'lower': 0, 'upper': 0, 'margin': 0}

  p = successes / attempts
  n = attempts
  z = stats.norm.ppf(1 - (1 - confidence) / 2)
  z_squared = z * z

  denominator = 1 + z_squared / n
  center = (p + z_squared / (2 * n)) / denominator
  margin = (z * math.sqrt(p * (1 - p) / n + z_squared / (4 * n * n))) / denominator

  return {
    'lower': max(0, center - margin),
    'upper': min(1, center + margin),
    'margin': margin
  }


def calculate_fbhe_ci(kills, errors, attempts, confidence=0.95):
  """Calculate confidence interval for FBHE metric."""
  if attempts == 0:
    return {
      'fbhe': 0,
      'lower': 0,
      'upper': 0,
      'margin': 0
    }

  fbhe = (kills - errors) / attempts

  # Transform FBHE from [-1, 1] to [0, 1] scale
  fbhe_scaled = (fbhe + 1) / 2
  successes_scaled = fbhe_scaled * attempts

  # Calculate CI
  ci = wilson_score_ci(successes_scaled, attempts, confidence)

  # Transform back to FBHE scale
  return {
    'fbhe': fbhe,
    'lower': ci['lower'] * 2 - 1,
    'upper': ci['upper'] * 2 - 1,
    'margin': ci['margin'] * 2
  }


def get_reliability_level(attempts):
  """Return reliability level based on sample size."""
  if attempts < 10:
    return "Very Limited"
  elif attempts < 20:
    return "Limited"
  elif attempts < 50:
    return "Moderate"
  elif attempts < 100:
    return "Good"
  else:
    return "Very Good"


#------------------------------------------------------------------------------------------------------
#                           Dataframe Creation Functions
#------------------------------------------------------------------------------------------------------

def create_margin_of_error_table():
  """Create reference table showing margin of error at different sample sizes."""

  sample_sizes = [5, 10, 15, 20, 30, 50, 75, 100, 150, 200]
  fbhe_values = [0.10, 0.20, 0.30, 0.40, 0.50]

  data = []
  for n in sample_sizes:
    row = {'Attempts': n}
    for fbhe_val in fbhe_values:
      # Estimate kills and errors for this FBHE
      kills = int(0.6 * n)
      errors = int(kills - fbhe_val * n)

      ci = calculate_fbhe_ci(kills, errors, n)
      row[f'FBHE={fbhe_val:.2f}'] = f"±{ci['margin']:.3f}"

    reliability = get_reliability_level(n)
    row['Reliability'] = reliability
    data.append(row)

  return pd.DataFrame(data)


def create_player_ci_metrics(ppr_df, disp_player, player_data_stats_df):
  """Create table of player's key metrics with confidence intervals."""

  # Filter for this player's attacks
  player_ppr = ppr_df[ppr_df['att_player'] == disp_player]

  if player_ppr.shape[0] == 0:
    return pd.DataFrame({'Message': ['No data available for this player']})

  metrics_data = []

  # FBHE - Attacking
  att_df = player_ppr[player_ppr['att_player'] == disp_player]
  if att_df.shape[0] > 0:
    fbhe_result = fbhe_obj(att_df, disp_player, 'att', False)
    ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)

    metrics_data.append({
      'Metric': 'FBHE (Attacking)',
      'Value': f"{ci['fbhe']:.3f}",
      '95% CI Lower': f"{ci['lower']:.3f}",
      '95% CI Upper': f"{ci['upper']:.3f}",
      'Margin (±)': f"{ci['margin']:.3f}",
      'Attempts': fbhe_result.attempts,
      'Reliability': get_reliability_level(fbhe_result.attempts)
    })

    # FBHE - Serving
  srv_df = player_ppr[player_ppr['serve_player'] == disp_player]
  if srv_df.shape[0] > 0:
    fbhe_result = fbhe_obj(srv_df, disp_player, 'srv', False)
    ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)

    metrics_data.append({
      'Metric': 'FBHE (Serving)',
      'Value': f"{ci['fbhe']:.3f}",
      '95% CI Lower': f"{ci['lower']:.3f}",
      '95% CI Upper': f"{ci['upper']:.3f}",
      'Margin (±)': f"{ci['margin']:.3f}",
      'Attempts': fbhe_result.attempts,
      'Reliability': get_reliability_level(fbhe_result.attempts)
    })

    # Good Pass Percentage
  pass_df = player_ppr[player_ppr['pass_player'] == disp_player]
  if pass_df.shape[0] > 0:
    oos_result = count_out_of_system(pass_df, disp_player, 'pass')
    good_passes = int((1 - oos_result[1]) * oos_result[2])

    ci = wilson_score_ci(good_passes, oos_result[2])

    metrics_data.append({
      'Metric': 'Good Pass %',
      'Value': f"{(1-oos_result[1])*100:.1f}%",
      '95% CI Lower': f"{ci['lower']*100:.1f}%",
      '95% CI Upper': f"{ci['upper']*100:.1f}%",
      'Margin (±)': f"{ci['margin']*100:.1f}%",
      'Attempts': oos_result[2],
      'Reliability': get_reliability_level(oos_result[2])
    })

  return pd.DataFrame(metrics_data)


def create_reliability_guidelines():
  """Create table of reliability guidelines."""

  data = [
    {
      'Attempts': '< 10',
      'Reliability': 'Very Limited',
      'Typical Margin': '> ±0.50',
      'Best Use': 'Early scouting, trend spotting only',
      'Decision Level': '⚠️ Use with extreme caution'
    },
    {
      'Attempts': '10-19',
      'Reliability': 'Limited',
      'Typical Margin': '±0.35 to ±0.50',
      'Best Use': 'Identify major patterns',
      'Decision Level': '⚠️ General tendencies only'
    },
    {
      'Attempts': '20-49',
      'Reliability': 'Moderate',
      'Typical Margin': '±0.25 to ±0.35',
      'Best Use': 'Game planning, lineup decisions',
      'Decision Level': '✓ Coaching decisions'
    },
    {
      'Attempts': '50-99',
      'Reliability': 'Good',
      'Typical Margin': '±0.15 to ±0.25',
      'Best Use': 'Season evaluation, recruiting',
      'Decision Level': '✓ Detailed analysis'
    },
    {
      'Attempts': '100+',
      'Reliability': 'Very Good',
      'Typical Margin': '< ±0.15',
      'Best Use': 'All purposes',
      'Decision Level': '✓✓ High confidence'
    }
  ]

  return pd.DataFrame(data)


def create_sample_size_requirements():
  """Create table showing required sample sizes for target precision."""

  data = []
  desired_margins = [0.20, 0.15, 0.10, 0.05, 0.03]

  for margin in desired_margins:
    # Calculate required n for proportion around 0.65 (scaled from FBHE 0.30)
    z = stats.norm.ppf(0.975)  # 95% CI
    p = 0.65
    n = int(math.ceil((z * z * p * (1 - p)) / (margin * margin)))

    # Estimate matches needed (assume ~25 attempts per match)
    matches = int(math.ceil(n / 25))

    timeframe = ""
    if matches <= 2:
      timeframe = "1-2 matches"
    elif matches <= 5:
      timeframe = "3-5 matches"
    elif matches <= 10:
      timeframe = "Half season"
    elif matches <= 15:
      timeframe = "Full season"
    else:
      timeframe = "Multiple seasons"

    data.append({
      'Desired Precision': f"±{margin:.2f} ({margin*100:.0f}%)",
      'Attempts Needed': f"~{n}",
      'Est. Matches': f"~{matches}",
      'Typical Timeframe': timeframe
    })

  return pd.DataFrame(data)



def create_zone_confidence_analysis(ppr_df, disp_player, player_data_stats_df):
  """Create table analyzing FBHE by zone with confidence intervals."""

  player_ppr = ppr_df[(ppr_df['att_player'] == disp_player)]

  if player_ppr.shape[0] == 0:
    return pd.DataFrame({'Message': ['No attacking data available']})

    # Check if zone column exists
  if 'att_src_zone_net' not in player_ppr.columns:
    return pd.DataFrame({'Message': ['Zone column not found in data']})

  zones_data = []

  # Loop through all 5 zones (1, 2, 3, 4, 5)
  for zone in [1, 2, 3, 4, 5]:
    zone_df = player_ppr[player_ppr['att_src_zone_net'] == zone]

    if zone_df.shape[0] > 0:
      try:
        fbhe_result = fbhe_obj(zone_df, disp_player, 'att', False)
        ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)

        zones_data.append({
          'Zone': f"Zone {zone}",
          'FBHE': f"{ci['fbhe']:.3f}",
          'CI Lower': f"{ci['lower']:.3f}",
          'CI Upper': f"{ci['upper']:.3f}",
          'Margin': f"±{ci['margin']:.3f}",
          'Attempts': fbhe_result.attempts,
          'Reliability': get_reliability_level(fbhe_result.attempts),
          'Kills': fbhe_result.kills,
          'Errors': fbhe_result.errors
        })
      except Exception as e:
        print(f"Error calculating zone {zone}: {e}")

    # Add overall (all zones combined)
  try:
    fbhe_result = fbhe_obj(player_ppr, disp_player, 'att', False)
    ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)

    zones_data.append({
      'Zone': 'All Zones',
      'FBHE': f"{ci['fbhe']:.3f}",
      'CI Lower': f"{ci['lower']:.3f}",
      'CI Upper': f"{ci['upper']:.3f}",
      'Margin': f"±{ci['margin']:.3f}",
      'Attempts': fbhe_result.attempts,
      'Reliability': get_reliability_level(fbhe_result.attempts),
      'Kills': fbhe_result.kills,
      'Errors': fbhe_result.errors
    })
  except Exception as e:
    print(f"Error calculating all zones: {e}")

  if not zones_data:
    return pd.DataFrame({'Message': ['No zone data available']})

  return pd.DataFrame(zones_data)
  
def create_cumulative_ci_analysis(ppr_df, disp_player):
  """Show how confidence intervals narrow over time."""

  player_ppr = ppr_df[(ppr_df['att_player'] == disp_player)]

  if player_ppr.shape[0] < 10:
    return pd.DataFrame({'Message': ['Insufficient data for cumulative analysis (need 10+ attempts)']})

    # Check if column exists
  if 'point_outcome' not in player_ppr.columns:
    return pd.DataFrame({'Message': ['Point outcome column not found']})

    # Sort by date if available
  if 'game_date' in player_ppr.columns:
    player_ppr = player_ppr.sort_values('game_date')

  data = []
  cumulative_kills = 0
  cumulative_errors = 0
  cumulative_attempts = 0

  # Sample at specific intervals
  intervals = [10, 20, 30, 50, 75, 100, 150, 200]
  current_interval_idx = 0

  for idx, row in player_ppr.iterrows():
    # Check for kill (FBK = First Ball Kill)
    if row['point_outcome'] == 'FBK':
      cumulative_kills += 1
      # Check for error (FBE = First Ball Error)
    elif row['point_outcome'] == 'FBE':
      cumulative_errors += 1

    cumulative_attempts += 1

    # Check if we've reached next interval
    if current_interval_idx < len(intervals) and cumulative_attempts >= intervals[current_interval_idx]:
      ci = calculate_fbhe_ci(cumulative_kills, cumulative_errors, cumulative_attempts)

      data.append({
        'After N Attempts': cumulative_attempts,
        'FBHE': f"{ci['fbhe']:.3f}",
        'CI Width': f"{ci['upper'] - ci['lower']:.3f}",
        'Margin': f"±{ci['margin']:.3f}",
        'Reliability': get_reliability_level(cumulative_attempts)
      })

      current_interval_idx += 1

    # Add final cumulative if not already added
  if data and cumulative_attempts not in [d['After N Attempts'] for d in data]:
    ci = calculate_fbhe_ci(cumulative_kills, cumulative_errors, cumulative_attempts)
    data.append({
      'After N Attempts': cumulative_attempts,
      'FBHE': f"{ci['fbhe']:.3f}",
      'CI Width': f"{ci['upper'] - ci['lower']:.3f}",
      'Margin': f"±{ci['margin']:.3f}",
      'Reliability': get_reliability_level(cumulative_attempts)
    })
  elif not data:
    # If no intervals were reached, add at least the final total
    ci = calculate_fbhe_ci(cumulative_kills, cumulative_errors, cumulative_attempts)
    data.append({
      'After N Attempts': cumulative_attempts,
      'FBHE': f"{ci['fbhe']:.3f}",
      'CI Width': f"{ci['upper'] - ci['lower']:.3f}",
      'Margin': f"±{ci['margin']:.3f}",
      'Reliability': get_reliability_level(cumulative_attempts)
    })

  if not data:
    return pd.DataFrame({'Message': ['Not enough data to reach any intervals']})

  return pd.DataFrame(data)
  


#------------------------------------------------------------------------------------------------------
#                           Plot/Image Creation Functions
#------------------------------------------------------------------------------------------------------
def plot_margin_vs_sample_size():
    """Create plot showing margin of error decreasing with sample size."""
    import plotly.graph_objects as go
    
    sample_sizes = list(range(5, 201, 5))
    margins = []
    
    for n in sample_sizes:
        # Assume typical FBHE around 0.30
        kills = int(0.6 * n)
        errors = int(kills - 0.30 * n)
        ci = calculate_fbhe_ci(kills, errors, n)
        margins.append(ci['margin'])
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=sample_sizes,
        y=margins,
        mode='lines+markers',
        name='Margin of Error',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=6)
    ))
    
    # Add reference lines
    fig.add_hline(y=0.35, line_dash="dash", line_color="red", 
                  annotation_text="Moderate (±0.35)", annotation_position="right")
    fig.add_hline(y=0.25, line_dash="dash", line_color="orange",
                  annotation_text="Good (±0.25)", annotation_position="right")
    fig.add_hline(y=0.18, line_dash="dash", line_color="green",
                  annotation_text="Very Good (±0.18)", annotation_position="right")
    
    fig.update_layout(
        title="How Margin of Error Decreases with Sample Size",
        xaxis_title="Number of Attempts",
        yaxis_title="Margin of Error (±)",
        hovermode='x unified',
        showlegend=True
    )
    
    return fig


def plot_player_ci_visualization(ppr_df, disp_player):
    """Create visualization of player's metrics with error bars."""
    import plotly.graph_objects as go
    
    player_ppr = ppr_df[ppr_df['att_player'] == disp_player]
    
    metrics = []
    values = []
    lower_errors = []
    upper_errors = []
    
    # Attacking
    att_df = player_ppr[player_ppr['att_player'] == disp_player]
    if att_df.shape[0] > 0:
        fbhe_result = fbhe_obj(att_df, disp_player, 'att', False)
        ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)
        
        metrics.append(f'Attacking\n(n={fbhe_result.attempts})')
        values.append(ci['fbhe'])
        lower_errors.append(ci['fbhe'] - ci['lower'])
        upper_errors.append(ci['upper'] - ci['fbhe'])
    
    # Serving
    srv_df = player_ppr[player_ppr['serve_player'] == disp_player]
    if srv_df.shape[0] > 0:
        fbhe_result = fbhe_obj(srv_df, disp_player, 'srv', False)
        ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)
        
        metrics.append(f'Serving\n(n={fbhe_result.attempts})')
        values.append(ci['fbhe'])
        lower_errors.append(ci['fbhe'] - ci['lower'])
        upper_errors.append(ci['upper'] - ci['fbhe'])
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=metrics,
        y=values,
        error_y=dict(
            type='data',
            symmetric=False,
            array=upper_errors,
            arrayminus=lower_errors
        ),
        marker_color='lightblue',
        name='FBHE with 95% CI'
    ))
    
    fig.update_layout(
        title=f"{disp_player} - FBHE with 95% Confidence Intervals",
        yaxis_title="FBHE",
        yaxis=dict(range=[-1, 1]),
        showlegend=False
    )
    
    return fig

def plot_zone_comparison_with_ci(ppr_df, disp_player):
  import plotly.graph_objects as go

  player_att = ppr_df[ppr_df['att_player'] == disp_player]

  # Use correct column name
  zone_col = 'att_src_zone_net'

  if zone_col not in player_att.columns:
    fig = go.Figure()
    fig.add_annotation(text="Zone data not available",
                       xref="paper", yref="paper",
                       x=0.5, y=0.5, showarrow=False)
    return fig

  zones = []
  fbhe_values = []
  lower_errors = []
  upper_errors = []
  colors = []

  # Zones are 1, 2, 3, 4, 5 in your system
  for zone in [1, 2, 3, 4, 5]:
    zone_df = player_att[player_att[zone_col] == zone]
    if zone_df.shape[0] > 0:
      try:
        fbhe_result = fbhe_obj(zone_df, disp_player, 'att', False)
        ci = calculate_fbhe_ci(fbhe_result.kills, fbhe_result.errors, fbhe_result.attempts)

        zones.append(f'Zone {zone}\n(n={fbhe_result.attempts})')
        fbhe_values.append(ci['fbhe'])
        lower_errors.append(ci['fbhe'] - ci['lower'])
        upper_errors.append(ci['upper'] - ci['fbhe'])

        reliability = get_reliability_level(fbhe_result.attempts)
        colors.append('green' if reliability in ['Good', 'Very Good'] else 
                            'orange' if reliability == 'Moderate' else 'red')
      except:
        pass
    
    if not zones:
        fig = go.Figure()
        fig.add_annotation(text="No zone data available",
                          xref="paper", yref="paper",
                          x=0.5, y=0.5, showarrow=False)
        return fig
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=zones, y=fbhe_values,
                         error_y=dict(type='data', symmetric=False,
                                    array=upper_errors, arrayminus=lower_errors),
                         marker_color=colors, name='FBHE by Zone'))
    
    fig.update_layout(title=f"{disp_player} - FBHE by Zone with 95% CI",
                     yaxis_title="FBHE", yaxis=dict(range=[-1, 1]), showlegend=False)
    return fig
  