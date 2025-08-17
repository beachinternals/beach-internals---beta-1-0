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

def report_player_att_along_net( lgy, team, **rpt_filters):
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
  #
  #            Initialize all lists, get and filter the data, and fetch in information from report_list
  #
  #-----------------------------------------------------------------------------------------------------
  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe, and/or player stats, and/or tri-data
  # comment some in our out based on this reports needs.
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # Filter the ppr dataframe
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)


  #------------------------------------------------------------------------------------------------------
  #
  #            Create the images and dataframes with this data
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
  att_table = get_player_attack_table(ppr_df, player_data_stats_df, disp_player)
  if att_table is None:
    df_list[0] = []  # Empty list for DataGrid
  else:
    # Convert problematic types
    for col in att_table.columns:
      if att_table[col].dtype == 'datetime64[ns]':
        att_table[col] = att_table[col].dt.strftime('%Y-%m-%d')  # Convert datetime to string
      elif att_table[col].dtype == object:
        att_table[col] = att_table[col].astype(str).replace('nan', None)  # Handle NaN, None
      elif att_table[col].dtype in ['int64', 'float64']:
        att_table[col] = att_table[col].astype(object).where(att_table[col].notna(), None)  # Numpy to Python
    df_list[0] = att_table.to_dict('records')

    print(f"Player Attacking Along Net, df0 = {df_list[0]}")


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


  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list



def get_player_attack_table(ppr_df, player_data_stats_df, disp_player):
  '''
  geenrates the dataframe for the player attacking table

  INPUTS:
    - ppr dataframe, limited as desired before calling
    - player for the dataframe

  RETURNS
    - the dataframe
    
  '''
  # create the output dataframe
  df_dict = {' ':['FBHE','Percentile','FBSO','Kills','Errors','Attempts','URL'],
             'All':[0,0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,0,' '],
             "Zone 2":[0,0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,0,' '],
             'Zone 4':[0,0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,0,' '],
             'No Zone':[0,0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  var_mean = 'fbhe'+'_mean'
  var_stdev = 'fbhe'+'_stdev'

  # if the eata is not empty, create my df, populate it, and return it
  if ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( ppr_df, disp_player, 'att', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[3,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[4,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[5,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[2,'All'] = fbhe_vector[4]  # fbso
    fbhe_table.at[6,'All'] = fbhe_vector[5]  # URL
    # calucalte percentile
    fbhe_table.at[1,'All'] = round( stats.norm.cdf((((fbhe_vector[0])- player_data_stats_df.at[0,var_mean])/(player_data_stats_df.at[0,var_stdev]))) , 3)
    fbhe_table.at[1,'All'] = str('{:.0%}').format(fbhe_table.at[1,'All'])

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5','No Zone']
    for i in [1,2,3,4,5,6]:
      zone = 0 if i == 6 else i
      if i != 6:
        var_mean = 'fbhe'+ str(zone)+'_mean'
        var_stdev = 'fbhe'+str(zone)+'_stdev'
      fbhe_vector = fbhe( ppr_df[ppr_df['att_src_zone_net']==zone], disp_player, 'att', True )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[3,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[4,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[5,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[2,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[6,column[i-1]] = fbhe_vector[5]  # URL
      fbhe_table.at[1,column[i-1]] = round( stats.norm.cdf((((fbhe_vector[0])- player_data_stats_df.at[0,var_mean])/(player_data_stats_df.at[0,var_stdev]))) , 3)
      fbhe_table.at[1,column[i-1]] = str('{:.0%}').format(fbhe_table.at[1,column[i-1]])

    return fbhe_table
  else:
    fbhe_table.at[0,'Zone 1'] = 'No Data Found'
    return fbhe_table



def get_player_attack_plots( ppr_df, disp_player):

  # limit the data to passes by the player
  new_ppr = ppr_df[ ppr_df['att_player'] == disp_player]

  # set up 5 sub plots
  attack_z1_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 1) & (new_ppr['tactic'] != 'option')],'att',1,'Attacks from Left Pin')  
  attack_z2_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 2) & (new_ppr['tactic'] != 'option')],'att',2, 'Attacks from Left Slot')
  attack_z3_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 3) & (new_ppr['tactic'] != 'option')],'att',3, 'Attacks from Middle')
  attack_z4_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 4) & (new_ppr['tactic'] != 'option')],'att',4, 'Attacks from Right Slot')
  attack_z5_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 5) & (new_ppr['tactic'] != 'option')],'att',5, 'Attacks from Right Pin')

  zone_dict = {'Metric':['FBHE','FBSO','ATT','URL'],'Value':[0,0,0,'']}
  z1_df = pd.DataFrame.from_dict(zone_dict)
  z2_df = pd.DataFrame.from_dict(zone_dict)
  z3_df = pd.DataFrame.from_dict(zone_dict)
  z4_df = pd.DataFrame.from_dict(zone_dict)
  z5_df = pd.DataFrame.from_dict(zone_dict)

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 1) & (new_ppr['tactic'] != 'option')], disp_player, 'att', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 1) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z1_df.at[0,'Value'] = fbhe_vector[0]
  z1_df.at[1,'Value'] = fbhe_vector[4]
  z1_df.at[2,'Value'] = fbhe_vector[3]
  z1_df.at[3,'Value'] = fbhe_vector[5]
  #z1_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 2) & (new_ppr['tactic'] != 'option')], disp_player, 'att', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 2) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z2_df.at[0,'Value'] = fbhe_vector[0]
  z2_df.at[1,'Value'] = fbhe_vector[4]
  z2_df.at[2,'Value'] = fbhe_vector[3]
  z2_df.at[3,'Value'] = fbhe_vector[5]
  #z2_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 3) & (new_ppr['tactic'] != 'option')], disp_player, 'att', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 3) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z3_df.at[0,'Value'] = fbhe_vector[0]
  z3_df.at[1,'Value'] = fbhe_vector[4]
  z3_df.at[2,'Value'] = fbhe_vector[3]
  z3_df.at[3,'Value'] = fbhe_vector[5]
  #z3_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 4) & (new_ppr['tactic'] != 'option')], disp_player, 'att', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 4) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z4_df.at[0,'Value'] = fbhe_vector[0]
  z4_df.at[1,'Value'] = fbhe_vector[4]
  z4_df.at[2,'Value'] = fbhe_vector[3]
  z4_df.at[3,'Value'] = fbhe_vector[5]
  #z4_df.at[3,'Value'] = oos_vector[0]

  fbhe_vector = fbhe(new_ppr[ (new_ppr['att_src_zone_net'] == 5) & (new_ppr['tactic'] != 'option')], disp_player, 'att', 'Yes')
  #oos_vector = count_out_of_system(new_ppr[ (new_ppr['att_src_zone_net'] == 5) & (new_ppr['tactic'] != 'option')], disp_player, 'pass' )
  z5_df.at[0,'Value'] = fbhe_vector[0]
  z5_df.at[1,'Value'] = fbhe_vector[4]
  z5_df.at[2,'Value'] = fbhe_vector[3]
  z5_df.at[3,'Value'] = fbhe_vector[5]
  #z5_df.at[3,'Value'] = oos_vector[0]

  #z1_mkdn = pd.DataFrame.to_markdown(z1_df, index=False, headers=['',''] )
  #z2_mkdn = pd.DataFrame.to_markdown(z2_df, index=False, headers=['',''])
  #z3_mkdn = pd.DataFrame.to_markdown(z3_df, index=False, headers=['',''])
  #z4_mkdn = pd.DataFrame.to_markdown(z4_df, index=False, headers=['',''])
  #z5_mkdn = pd.DataFrame.to_markdown(z5_df, index=False, headers=['',''])

  # now change the column names
  # Rename the 'Metric' column based on DataFrame name
  z1_df = z1_df.rename(columns={'Metric': 'Zone 1'})
  z2_df = z2_df.rename(columns={'Metric': 'Zone 2'})
  z3_df = z3_df.rename(columns={'Metric': 'Zone 3'})
  z4_df = z4_df.rename(columns={'Metric': 'Zone 4'})
  z5_df = z5_df.rename(columns={'Metric': 'Zone 5'})

  return attack_z1_plot_object, attack_z2_plot_object, attack_z3_plot_object, attack_z4_plot_object, attack_z5_plot_object, z1_df, z2_df, z3_df, z4_df, z5_df




def report_player_att_tendencies(lgy, team, **rpt_filters):
  """
  Test report function - serves as a stub/template for other report functions.
  
  Args:
    lgy: League+gender+year string
    team: Team identifier
    **rpt_filters: Additional report filters
    
  Returns:
    tuple: (title_list, label_list, image_list, df_list, df_desc_list, image_desc_list)
  """
  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe, and/or player stats, and/or tri-data
  # comment some in our out based on this reports needs.
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # Filter the ppr dataframe
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================

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

  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


#---------------------------------------------------------------------------
#              player report player 
#---------------------------------------------------------------------------
def report_player_att_set(lgy, team, **rpt_filters):
  """
  Test report function - serves as a stub/template for other report functions.
  
  Args:
    lgy: League+gender+year string
    team: Team identifier
    **rpt_filters: Additional report filters
    
  Returns:
    tuple: (title_list, label_list, image_list, df_list, df_desc_list, image_desc_list)
  """
  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe, and/or player stats, and/or tri-data
  # comment some in our out based on this reports needs.
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # Filter the ppr dataframe
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================

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

  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


def report_player_att_transition(lgy, team, **rpt_filters):
  """
  Test report function - serves as a stub/template for other report functions.
  
  Args:
    lgy: League+gender+year string
    team: Team identifier
    **rpt_filters: Additional report filters
    
  Returns:
    tuple: (title_list, label_list, image_list, df_list, df_desc_list, image_desc_list)
  """
  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe, and/or player stats, and/or tri-data
  # comment some in our out based on this reports needs.
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # Filter the ppr dataframe
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================

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
  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list



def report_player_att_expected(lgy, team, **rpt_filters):
  """
  Test report function - serves as a stub/template for other report functions.
  
  Args:
    lgy: League+gender+year string
    team: Team identifier
    **rpt_filters: Additional report filters
    
  Returns:
    tuple: (title_list, label_list, image_list, df_list, df_desc_list, image_desc_list)
  """
  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe, and/or player stats, and/or tri-data
  # comment some in our out based on this reports needs.
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)
  #tri_df, tri_df_found = get_tri_data( disp_league, disp_gender, disp_year, False, None, None ) #date checked, start date, end date

  # Filter the ppr dataframe
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================
  

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

  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list



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
