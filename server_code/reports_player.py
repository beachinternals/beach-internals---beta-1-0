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
from scipy.stats import chi2, norm
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
#from matchup_reports import player_45_serves
from plot_functions import *
from datetime import datetime, timedelta
from server_functions import *
from plot_functions import *

# Create logger with formatting
from anvil_extras.logging import Logger
import logging
logger = Logger()
# If the library supports standard Python logging formatting:
formatter = logging.Formatter('%(levelname)s - %(funcName)s:%(lineno)d - %(message)s')

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
  df_dict = {' ':['FBHE','Percentile','FBSO','Kills','Errors','Attempts','Percent Errors','URL'],
             'All':[0,0,0,0,0,0,0,' '],
             'Area 1':[0,0,0,0,0,0,0,' '],
             "Area 2":[0,0,0,0,0,0,0,' '],
             'Area 3':[0,0,0,0,0,0,0,' '],
             'Area 4':[0,0,0,0,0,0,0,' '],
             'Area 5':[0,0,0,0,0,0,0,' '],
             'No Area':[0,0,0,0,0,0,0,' ']
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
    fbhe_table.at[7,'All'] = fbhe_vector[5]  # URL
    # calculate percent errors
    if fbhe_vector[3] > 0:  # attempts
      fbhe_table.at[6,'All'] = str('{:.0%}').format(fbhe_vector[2] / fbhe_vector[3])  # errors / attempts
    else:
      fbhe_table.at[6,'All'] = '0%'
    # calucalte percentile
    fbhe_table.at[1,'All'] = round( stats.norm.cdf((((fbhe_vector[0])- player_data_stats_df.at[0,var_mean])/(player_data_stats_df.at[0,var_stdev]))) , 3)
    fbhe_table.at[1,'All'] = str('{:.0%}').format(fbhe_table.at[1,'All'])
    # calculate for areas 1 - 5
    column = ['Area 1','Area 2','Area 3','Area 4','Area 5','No Area']
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
      fbhe_table.at[7,column[i-1]] = fbhe_vector[5]  # URL
      # calculate percent errors
      if fbhe_vector[3] > 0:  # attempts
        fbhe_table.at[6,column[i-1]] = str('{:.0%}').format(fbhe_vector[2] / fbhe_vector[3])  # errors / attempts
      else:
        fbhe_table.at[6,column[i-1]] = '0%'
      fbhe_table.at[1,column[i-1]] = round( stats.norm.cdf((((fbhe_vector[0])- player_data_stats_df.at[0,var_mean])/(player_data_stats_df.at[0,var_stdev]))) , 3)
      fbhe_table.at[1,column[i-1]] = str('{:.0%}').format(fbhe_table.at[1,column[i-1]])
    return fbhe_table
  else:
    fbhe_table.at[0,'Area 1'] = 'No Data Found'
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
  z1_df = z1_df.rename(columns={'Metric': 'Area 1'})
  z2_df = z2_df.rename(columns={'Metric': 'Area 2'})
  z3_df = z3_df.rename(columns={'Metric': 'Area 3'})
  z4_df = z4_df.rename(columns={'Metric': 'Area 4'})
  z5_df = z5_df.rename(columns={'Metric': 'Area 5'})

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
  trans_obj_all = calc_trans_obj(ppr_df, disp_player, 'att')
  if trans_obj_all['status']:
    table_data['All'][0] = trans_obj_all['tcr_str']  # Transition Conversion
    _, table_data['All'][1] = calculate_percentile(trans_obj_all['tcr'], player_data_stats_df.at[0,'tcr_mean'], player_data_stats_df.at[0,'tcr_stdev'])  # Percentile
    table_data['All'][2] = trans_obj_all['t_eff_str']  # Transition Effectiveness
    _, table_data['All'][3] = calculate_percentile(trans_obj_all['t_eff'], player_data_stats_df.at[0,'t_eff_mean'], player_data_stats_df.at[0,'t_eff_stdev'])  # Percentile
    table_data['All'][4] = trans_obj_all['t_create_str']  # Transition Creates
    percent, _ = calculate_percentile(trans_obj_all['t_create'], player_data_stats_df.at[0,'t_create_mean'], player_data_stats_df.at[0,'t_create_stdev'])  # Percentile
    table_data['All'][5] =  f"{1-percent:.0%}"
    table_data['All'][6] = str(trans_obj_all['tran_total_pts'])  # Transition Points

  # Calculate metrics for each area (1 to 5)
  for area in range(1, 6):
    area_df = ppr_df[ppr_df['att_src_zone_net'] == area]
    trans_obj_area = calc_trans_obj(area_df, disp_player, 'att')
    if trans_obj_area['status']:
      table_data[f'Area {area}'][0] = trans_obj_area['tcr_str']
      _, table_data[f'Area {area}'][1] = calculate_percentile(trans_obj_area['tcr'], player_data_stats_df.at[0,'tcr_mean'], player_data_stats_df.at[0,'tcr_stdev'])
      table_data[f'Area {area}'][2] = trans_obj_area['t_eff_str']
      _, table_data[f'Area {area}'][3] = calculate_percentile(trans_obj_area['t_eff'], player_data_stats_df.at[0,'t_eff_mean'], player_data_stats_df.at[0,'t_eff_stdev'])
      table_data[f'Area {area}'][4] = trans_obj_area['t_create_str']
      percent, _ = calculate_percentile(trans_obj_area['t_create'], player_data_stats_df.at[0,'t_create_mean'], player_data_stats_df.at[0,'t_create_stdev'])
      table_data[f'Area {area}'][5] = f"{1-percent:.0%}"
      table_data[f'Area {area}'][6] = str(trans_obj_area['tran_total_pts'])

  # Calculate metrics for 'No Area'
  no_area_df = ppr_df[ (ppr_df['att_src_zone_net'] != 1) & 
    (ppr_df['att_src_zone_net'] != 2) & 
    (ppr_df['att_src_zone_net'] != 3) & 
    (ppr_df['att_src_zone_net'] != 4) & 
    (ppr_df['att_src_zone_net'] != 5) 
    ]
  trans_obj_no_area = calc_trans_obj(no_area_df, disp_player, 'att')
  if trans_obj_no_area['status']:
    table_data['No Area'][0] = trans_obj_no_area['tcr_str']
    _, table_data['No Area'][1] = calculate_percentile(trans_obj_no_area['tcr'], player_data_stats_df.at[0,'tcr_mean'], player_data_stats_df.at[0,'tcr_stdev'])
    table_data['No Area'][2] = trans_obj_no_area['t_eff_str']
    _, table_data['No Area'][3] = calculate_percentile(trans_obj_no_area['t_eff'], player_data_stats_df.at[0,'t_eff_mean'], player_data_stats_df.at[0,'t_eff_stdev'])
    table_data['No Area'][4] = trans_obj_no_area['t_create_str']
    percent, _ = calculate_percentile(trans_obj_no_area['t_create'], player_data_stats_df.at[0,'t_create_mean'], player_data_stats_df.at[0,'t_create_stdev'])
    table_data['No Area'][5] = f"{1-percent:.0%}"
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
    'Metric': ['FBHE', 'Percentile', 'First Ball Attempts', 'Transition Conversion', 'Percentile', 'Transition Attempts', 'Expected Value', 'Percentile', 'First Ball & Transition Attempts'],
    'All': [0.0, 0.0, 0, 0.0, 0.0, 0, '0.00%', 0.0, 0],
    'Area 1': [0.0, 0.0, 0, 0.0, 0.0, 0, '0.00%', 0.0, 0],
    'Area 2': [0.0, 0.0, 0, 0.0, 0.0, 0, '0.00%', 0.0, 0],
    'Area 3': [0.0, 0.0, 0, 0.0, 0.0, 0, '0.00%', 0.0, 0],
    'Area 4': [0.0, 0.0, 0, 0.0, 0.0, 0, '0.00%', 0.0, 0],
    'Area 5': [0.0, 0.0, 0, 0.0, 0.0, 0, '0.00%', 0.0, 0],
    'No Area': [0.0, 0.0, 0, 0.0, 0.0, 0, '0.00%', 0.0, 0]
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

    # Calculate First Ball Attempts
    table_data[area][2] = int(fbhe_result.attempts)

    # Calculate Transition Conversion
    trans_result = calc_trans_obj(temp_df, disp_player, flag='att')
    table_data[area][3] = trans_result.get('tcr_str', 0.0)
    tcr_value = trans_result.get('tcr', 0.0)
    # Use percentile_str from calculate_percentile
    _, tcr_percentile_str = calculate_percentile(
      tcr_value,
      player_data_stats_df.at[0, 'tcr_mean'],
      player_data_stats_df.at[0, 'tcr_stdev']
    )
    table_data[area][4] = tcr_percentile_str if tcr_percentile_str is not None else '0%'

    # Calculate Transition Attempts
    table_data[area][5] = int(trans_result.get('tran_total_pts', 0))

    # Calculate Expected Value
    ev_result = calc_ev_obj(temp_df, disp_player)
    table_data[area][6] = ev_result.get('expected_value', '0%')
    # Convert percentage string to float for percentile calculation
    ev_value = float(ev_result.get('expected_value', '0%').strip('%')) 
    #print(f" ev_value {ev_value}, mean {player_data_stats_df.at[0, 'expected_mean']}, stdev {player_data_stats_df.at[0, 'expected_stdev']}")
    _, ev_percentile_str = calculate_percentile(
      ev_value,
      player_data_stats_df.at[0, 'expected_mean'],
      player_data_stats_df.at[0, 'expected_stdev']
    )
    table_data[area][7] = ev_percentile_str if ev_percentile_str is not None else '0%'

    # Calculate First Ball & Transition Attempts
    table_data[area][8] = int(fbhe_result.attempts) + int(trans_result.get('tran_total_pts', 0))

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




#------------------------------------------------------
#  Player Passing Reports
#=-------------------------------------------------------------
def player_other_sw(lgy, team, **rpt_filters):
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

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]

  # unpack player into team, number and short name
  str_loc = disp_player.index(' ')
  p_team = disp_player[:str_loc].strip()
  p_player = disp_player[str_loc+1:]
  str_loc = p_player.index(' ')
  p_num = p_player[:str_loc].strip()
  p_sname = p_player[str_loc+1:].strip()
  
  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================

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
  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list





def report_player_passing_45_fbhe(lgy, team, **rpt_filters):
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

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================

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


  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list



def report_player_passing_45_pass(lgy, team, **rpt_filters):
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

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================
  
  # firt, this reprot is only when the player is passing, so:
  #ppr_df = ppr_df[ppr_df['pass_player'] == disp_player] 

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

    el_result = find_ellipse_area(ppr_df, disp_player, 'pass', min_att=5)
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

      el_result = find_ellipse_area(ppr_df[ppr_df['serve_src_zone_net']==zone], disp_player, 'pass', min_att=5)
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
                                    disp_player, 'pass', min_att=5
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
                                    disp_player, 'pass', min_att=5
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
                                    disp_player, 'pass', min_att=5
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

  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


def report_player_passing_cluster(lgy, team, **rpt_filters):
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

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================


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
  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


def report_player_srv_fbhe(lgy, team, **rpt_filters):
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

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================

  #------------------------------------------------------------------------------------
  #
  #     Report is 'set up', not calcualte acorss the 3 zones, then the 45 serves
  #
  #-------------------------------------------------------------------------------------

  # create the output dataframe - This dataframe is the summary for zone 1,3,5
  df_dict = {' ':['FBHE','Percentile','FBSO','Kills','Errors','Attempts','% In System','Percentile','URL'],
             'All':[0,0,0,0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,0,0,0,' '],
             'No Zone':[0,0,0,0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( ppr_df, disp_player, 'srv', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[3,'All'] = fbhe_vector[1]  # kills
    fbhe_table.at[4,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[5,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[2,'All'] = fbhe_vector[4]  # FBSO
    fbhe_table.at[8,'All'] = fbhe_vector[5]  # URL
    oos_vector = count_out_of_system( ppr_df, disp_player, 'srv' )
    fbhe_table.at[6,'All'] = 1 - oos_vector[1]  # Good Pass
    fbhe_table.at[6,'All'] = str('{:.1%}').format(fbhe_table.at[6,'All'])
    # FBHE Percentile
    fbhe_table.at[1,'All'] =  round( stats.norm.cdf( (fbhe_vector[0] - player_data_stats_df.at[0,'srv_fbhe_mean'])/ player_data_stats_df.at[0,'srv_fbhe_stdev'] ), 3)
    fbhe_table.at[1,'All'] = str('{:.0%}').format(fbhe_table.at[1,'All'])
    value = fbhe_table.at[6, 'All']  # '89.3%'
    float_value = float(value.replace('%', ''))/100  # 89.3
    fbhe_table.at[7,'All'] =  round( stats.norm.cdf( (float_value - player_data_stats_df.at[0,'goodpass_mean'])/ player_data_stats_df.at[0,'goodpass_stdev'] ), 3)
    fbhe_table.at[7,'All'] = str('{:.0%}').format(fbhe_table.at[7,'All'])

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 3','Zone 5','No Zone']
    zone_stats = ['srv1_fbhe_mean', 'srv3_fbhe_mean', 'srv5_fbhe_mean', 'srv_fbhe_mean']
    zone_stdev = ['srv1_fbhe_stdev', 'srv3_fbhe_stdev', 'srv5_fbhe_stdev', 'srv_fbhe_stdev']
    for i in [0,1,2,3]:
      zone = 0 if i == 3 else (i*2)+1
      fbhe_vector = fbhe( ppr_df[ppr_df['serve_src_zone_net']==zone], disp_player, 'srv', True )
      fbhe_table.at[0,column[i]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[3,column[i]] = fbhe_vector[1]  # kills
      fbhe_table.at[4,column[i]] = fbhe_vector[2]  # errors
      fbhe_table.at[5,column[i]] = fbhe_vector[3]  # attempts
      fbhe_table.at[2,column[i]] = fbhe_vector[4]  # fbso
      fbhe_table.at[8,column[i]] = fbhe_vector[5]  # URL
      oos_vector = count_out_of_system( ppr_df[ppr_df['serve_src_zone_net']==zone], disp_player, 'srv' )
      fbhe_table.at[6,column[i]] = 1 - oos_vector[1]  # Good Pass
      fbhe_table.at[6,column[i]] = str('{:.1%}').format(fbhe_table.at[6,column[i]])
      fbhe_table.at[1,column[i]] =  round( stats.norm.cdf( (fbhe_vector[0] - player_data_stats_df.at[0,zone_stats[i]])/ player_data_stats_df.at[0,zone_stdev[i]] ), 3)
      fbhe_table.at[1,column[i]] = str('{:.0%}').format(fbhe_table.at[1,column[i]])
      value = fbhe_table.at[6,column[i]]  # '89.3%'
      float_value = float(value.replace('%', ''))/100  # 89.3
      fbhe_table.at[7,column[i]] =  round( stats.norm.cdf( (float_value - player_data_stats_df.at[0,'goodpass_mean'])/ player_data_stats_df.at[0,'goodpass_stdev'] ), 3)
      fbhe_table.at[7,column[i]] = str('{:.0%}').format(fbhe_table.at[7,column[i]])
  else:
    fbhe_table.at[0,'All'] = "No Data Found"

    # get high and low for the color scheme, mean +/- 2 sdtd
  cmax = player_data_stats_df.at[0,'srv_fbhe_mean']+2*player_data_stats_df.at[0,'srv_fbhe_stdev']
  cmin = player_data_stats_df.at[0,'srv_fbhe_mean']-2*player_data_stats_df.at[0,'srv_fbhe_stdev']

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
  srv1_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  srv3_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  srv5_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]

  # create the output dataframe - This dataframe is the for a detail below each graph, 1,3,5, so we can see the fnhe #, and the URL
  df_dict = {'Dest Zone':[' '],
             'FBHE':[0],
             'Srv':[0],
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
                         disp_player, 'srv', True
                        )
      #print(f"FBHE vector for 1, {i}{j}, {fbhe_vector}")
      if fbhe_vector[3] >= 5:
        pass1_val[index] = fbhe_vector[0]
        srv1_val[index] = fbhe_vector[3]
        z1_table.loc[z1_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z1_table.loc[z1_table_index,'FBHE'] = fbhe_vector[0]
        z1_table.loc[z1_table_index,'Srv'] = fbhe_vector[3]
        z1_table.loc[z1_table_index,'URL'] = fbhe_vector[5]
        z1_table_index = z1_table_index + 1


      # Zone 3
      fbhe_vector = fbhe(ppr_df[  (ppr_df['serve_src_zone_net'] == 3) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize())],
                         disp_player, 'srv', True
                        )
      if fbhe_vector[3] >= 5:        
        pass3_val[index] = fbhe_vector[0]
        srv3_val[index] = fbhe_vector[3]
        z3_table.loc[z3_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z3_table.loc[z3_table_index,'FBHE'] = fbhe_vector[0]
        z3_table.loc[z3_table_index,'Srv'] = fbhe_vector[3]
        z3_table.loc[z3_table_index,'URL'] = fbhe_vector[5]
        z3_table_index = z3_table_index + 1

      # Zone 5
      fbhe_vector = fbhe(ppr_df[  (ppr_df['serve_src_zone_net'] == 5) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                         disp_player, 'srv', True
                        )
      if fbhe_vector[3] >= 5:      
        pass5_val[index] = fbhe_vector[0]
        srv5_val[index] = fbhe_vector[3]
        z5_table.loc[z5_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z5_table.loc[z5_table_index,'FBHE'] = fbhe_vector[0]
        z5_table.loc[z5_table_index,'Srv'] = fbhe_vector[3]
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
  cmax = player_data_stats_df.at[0,'srv_fbhe_mean']+2*player_data_stats_df.at[0,'srv_fbhe_stdev']
  cmin = player_data_stats_df.at[0,'srv_fbhe_mean']-2*player_data_stats_df.at[0,'srv_fbhe_stdev']

  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x11, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass1_val, vmin=cmin, vmax=cmax, cmap='PiYG' )  
  # Add title with large font
  ax.set_title("Opponent's FBHE from Zone 1", fontsize=35)
  z1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass3_val, vmin=cmin, vmax=cmax, cmap='PiYG' ) 
  # Add title with large font
  ax.set_title("Opponent's FBHE from Zone 3", fontsize=35)
  z3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass5_val, vmin=cmin, vmax=cmax, cmap='PiYG' )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap='PiYG'),ax=ax, orientation='vertical', label='First Ball Hitting Efficiency')
  # Add title with large font
  ax.set_title("Opponent's FBHE from Zone 5", fontsize=35)
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
  print(f"Serves values 1 {srv1_val}")
  ax.plot( [x11, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=srv1_val, vmin=cmin, vmax=cmax, cmap='PiYG' ) 
  ax.set_title("Serves from Zone 1, Left", fontsize=35)
  s1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  print(f"Serves values 3 {srv3_val}")
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=srv3_val, vmin=cmin, vmax=cmax, cmap='PiYG' ) 
  ax.set_title("Serves from Zone 3, Middle", fontsize=35)
  s3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  print(f"Serves values 5 {srv5_val}")
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=srv5_val, vmin=cmin, vmax=cmax, cmap='PiYG' )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap='PiYG'),ax=ax, orientation='vertical', label='Serves')
  ax.set_title("Serves from Zone 5, Right", fontsize=35)
  s5_plt = anvil.mpl_util.plot_image()

  image_list[0] = s1_plt
  image_list[1] = s3_plt
  image_list[2] = s5_plt

  plt.close('All')


  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list

def report_player_srv_passing(lgy, team, **rpt_filters):
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

  # this is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # This is where you would customize for each different report function
  # =============================================================================

  # firt, this reprot is only when the player is serving, so:
  ppr_df = ppr_df[ppr_df['serve_player'] == disp_player] 


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
  # action is 'pass', 'srv', 'srv'
  # returns a vector : oos_vector[0] = number OOS (int), oos_vector[1] = percent out of system (Float()), oos_vector[2] = attempts (int())

  ############### Third Populate the dataframe, assuming we have data returned
  if ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = count_out_of_system(ppr_df, disp_player, 'srv')
    fbhe_table.at[0,'All'] = fbhe_vector[0]  #number out of system,
    fbhe_table.at[1,'All'] = str('{:.1%}').format(fbhe_vector[1])  # percent out of system
    fbhe_table.at[2,'All'] = round( stats.norm.cdf((((1-fbhe_vector[1])- player_data_stats_df.at[0,'goodpass_mean'])/(player_data_stats_df.at[0,'goodpass_stdev']))) , 3)
    fbhe_table.at[2,'All'] = str('{:.0%}').format(fbhe_table.at[2,'All'])
    fbhe_table.at[3,'All'] = fbhe_vector[2]  # attempts
    #fbhe_table.at[3,'All'] = fbhe_vector[3]  # URL (someday?)

    el_result = find_ellipse_area(ppr_df, disp_player, 'srv', min_att=5)
    if el_result.get('attempts') >= 5:
      area_table.at[0,'All'] = str('{:.1f}').format(el_result.get('area'))
      area_table.at[2,'All'] = el_result.get('attempts')
      #area_table.at[1,'All'] = round( ((el_result.get('area')- player_data_stats_df.at[0,'pass_ea_mean'])/(player_data_stats_df.at[0,'pass_es_stdev'])) , 3)
      area_table.at[3,'All'] = el_result.get('URL')  
      el_area.append(el_result.get('area'))



    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 3','Zone 5','No Zone']
    zone_goodpass_stats = ['goodpass_mean', 'goodpass_mean', 'goodpass_mean', 'goodpass_mean']
    zone_goodpass_stdev = ['goodpass_stdev', 'goodpass_stdev', 'goodpass_stdev', 'goodpass_stdev']
    for i in [0,1,2,3]:
      zone = 0 if i == 3 else (i*2)+1
      fbhe_vector = count_out_of_system( ppr_df[ppr_df['serve_src_zone_net']==zone], disp_player, 'srv')
      fbhe_table.at[0,column[i]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i]] = str('{:.1%}').format(fbhe_vector[1])  # attacks
      fbhe_table.at[2,column[i]] = round( stats.norm.cdf((((1-fbhe_vector[1])- player_data_stats_df.at[0,zone_goodpass_stats[i]])/(player_data_stats_df.at[0,zone_goodpass_stdev[i]]))) , 3)
      fbhe_table.at[2,column[i]] = str('{:.0%}').format(fbhe_table.at[2,column[i]])
      fbhe_table.at[3,column[i]] = fbhe_vector[2]  # errors
      #fbhe_table.at[3,column[i]] = fbhe_vector[3]  # URL someday

      el_result = find_ellipse_area(ppr_df[ppr_df['serve_src_zone_net']==zone], disp_player, 'srv', min_att=5)
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
             'Srv':[0],
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
              'Srv':[0],
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
                                        disp_player, 'srv'
                                       )
      #print(f"OOS vector for 1, {i}{j}, {fbhe_vector}")
      if fbhe_vector[2] >= 5:
        pass1_val[index] = fbhe_vector[1]
        z1_table.loc[z1_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z1_table.loc[z1_table_index,'Out Sys'] = str('{:.1%}').format(fbhe_vector[1])
        z1_table.loc[z1_table_index,'Srv'] = fbhe_vector[2]
        z1_table.loc[z1_table_index,'URL'] = ' '
        z1_table_index = z1_table_index + 1

        # Zone 1 Area
      el_result = find_ellipse_area(ppr_df[  (ppr_df['serve_src_zone_net'] == 1) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() ) &
        (ppr_df['serve_player'] == disp_player) ],
                                    disp_player, 'pass', min_att=5
                                   )
      print(f"el result for zone 1: attempts: {el_result.get('attempts')}, area: {el_result.get('area')}")
      if el_result.get('attempts') >= 5:
        area1_val[index] = el_result.get('area')
        a1_table.loc[a1_table_index,'Dest Zone'] = str(i)+j.capitalize()
        a1_table.loc[a1_table_index,'Area'] = str('{:.1f}').format(el_result.get('area'))
        a1_table.loc[a1_table_index,'Srv'] = el_result.get('attempts')
        a1_table.loc[a1_table_index,'URL'] = el_result.get('URL')
        el_area.append(el_result.get('area'))
        a1_table_index = a1_table_index + 1

      # Zone 3
      fbhe_vector = count_out_of_system(ppr_df[  (ppr_df['serve_src_zone_net'] == 3) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize())],
                                        disp_player, 'srv'
                                       )
      if fbhe_vector[2] >= 5:        
        pass3_val[index] = fbhe_vector[1]
        z3_table.loc[z3_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z3_table.loc[z3_table_index,'Out Sys'] = str('{:.1%}').format(fbhe_vector[1])
        z3_table.loc[z3_table_index,'Srv'] = fbhe_vector[2]
        z3_table.loc[z3_table_index,'URL'] = ' '
        z3_table_index = z3_table_index + 1

        # Zone 3 Area
      el_result = find_ellipse_area(ppr_df[  (ppr_df['serve_src_zone_net'] == 3) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() ) & 
        (ppr_df['serve_player'] == disp_player) ],
                                    disp_player, 'pass', min_att=5
                                   )
      #print(f"el result for zone 1: attempts: {el_result.get('attempts')}, area: {el_result.get('area')}")
      if el_result.get('attempts') >= 5:
        area3_val[index] = el_result.get('area')
        a3_table.loc[a3_table_index,'Dest Zone'] = str(i)+j.capitalize()
        a3_table.loc[a3_table_index,'Area'] = str('{:.1f}').format(el_result.get('area'))
        a3_table.loc[a3_table_index,'Srv'] = el_result.get('attempts')
        a3_table.loc[a3_table_index,'URL'] = el_result.get('URL')
        el_area.append(el_result.get('area'))
        a3_table_index = a3_table_index + 1

      # Zone 5
      fbhe_vector = count_out_of_system(ppr_df[  (ppr_df['serve_src_zone_net'] == 5) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() ) &
        (ppr_df['serve_player'] == disp_player) ],
                                        disp_player, 'srv'
                                       )
      if fbhe_vector[2] >= 5:      
        pass5_val[index] = fbhe_vector[1]
        z5_table.loc[z5_table_index,'Dest Zone'] = str(i)+j.capitalize()
        z5_table.loc[z5_table_index,'Out Sys'] = str('{:.1%}').format(fbhe_vector[1])
        z5_table.loc[z5_table_index,'Srv'] = fbhe_vector[2]
        z5_table.loc[z5_table_index,'URL'] = ' '
        z5_table_index = z5_table_index + 1

        # Zone 5 Area
      el_result = find_ellipse_area(ppr_df[  (ppr_df['serve_src_zone_net'] == 5) &
        (ppr_df['serve_dest_zone_net'] == i) &
        (ppr_df['serve_dest_zone_depth'] == j.capitalize() )],
                                    disp_player, 'pass', min_att=5
                                   )
      #print(f"el result for zone 1: attempts: {el_result.get('attempts')}, area: {el_result.get('area')}")
      if el_result.get('attempts') >= 5:
        area5_val[index] = el_result.get('area')
        a5_table.loc[a5_table_index,'Dest Zone'] = str(i)+j.capitalize()
        a5_table.loc[a5_table_index,'Area'] = str('{:.1f}').format(el_result.get('area'))
        a5_table.loc[a5_table_index,'Srv'] = el_result.get('attempts')
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
  ax.set_title("Opponent Good Passes from Zone 1", fontsize=35)
  z1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass3_val, vmin=cmin, vmax=cmax, cmap=custom_cmap ) 
  ax.set_title("Opponent Good Passes from Zone 3", fontsize=35)
  z3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass5_val, vmin=cmin, vmax=cmax, cmap=custom_cmap )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap=custom_cmap),ax=ax, orientation='vertical', label='Percent Good Passes')
  ax.set_title("Opponent Good Passes from Zone 5", fontsize=35)
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
  ax.set_title("Opponent's Pass Area from Zone 1", fontsize=40)
  a1_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 3 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=area3_val, vmin=cmin, vmax=cmax, cmap=custom_cmap ) 
  ax.set_title("Opponent's Pass Area from Zone 3", fontsize=35)
  a3_plt = anvil.mpl_util.plot_image()

  # Create the plot for serves from Zone 5 - define the figure, plot the court, plot a few serve lines, plot the dots
  fig, ax = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig,ax)
  ax.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=area5_val, vmin=cmin, vmax=cmax, cmap=custom_cmap )  
  fig.colorbar(mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(cmin, cmax), cmap=custom_cmap),ax=ax, orientation='vertical', label='Pass Area')
  ax.set_title("Opponent's Pass Area from Zone 5", fontsize=35)
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

  plt.close('All')

  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


def report_player_srv_transition(lgy, team, **rpt_filters):
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
  ppr_df = ppr_df[ppr_df['serve_player'].str.strip() == disp_player.strip()]

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
  trans_obj_all = calc_trans_obj(ppr_df, disp_player, 'srv')
  if trans_obj_all['status']:
    table_data['All'][0] = trans_obj_all['tcr_str']  # Transition Conversion
    _, table_data['All'][1] = calculate_percentile(trans_obj_all['tcr'], player_data_stats_df.at[0,'tcr_mean'], player_data_stats_df.at[0,'tcr_stdev'])  # Percentile
    table_data['All'][2] = trans_obj_all['t_eff_str']  # Transition Effectiveness
    _, table_data['All'][3] = calculate_percentile(trans_obj_all['t_eff'], player_data_stats_df.at[0,'t_eff_mean'], player_data_stats_df.at[0,'t_eff_stdev'])  # Percentile
    table_data['All'][4] = trans_obj_all['t_create_str']  # Transition Creates
    percent, _ = calculate_percentile(trans_obj_all['t_create'], player_data_stats_df.at[0,'t_create_mean'], player_data_stats_df.at[0,'t_create_stdev'])  # Percentile
    table_data['All'][5] =  f"{1-percent:.0%}"
    table_data['All'][6] = str(trans_obj_all['tran_total_pts'])  # Transition Points

  # Calculate metrics for each area (1 to 5)
  for area in range(1, 6):
    area_df = ppr_df[ppr_df['att_src_zone_net'] == area]
    trans_obj_area = calc_trans_obj(area_df, disp_player, 'srv')
    if trans_obj_area['status']:
      table_data[f'Area {area}'][0] = trans_obj_area['tcr_str']
      _, table_data[f'Area {area}'][1] = calculate_percentile(trans_obj_area['tcr'], player_data_stats_df.at[0,'tcr_mean'], player_data_stats_df.at[0,'tcr_stdev'])
      table_data[f'Area {area}'][2] = trans_obj_area['t_eff_str']
      _, table_data[f'Area {area}'][3] = calculate_percentile(trans_obj_area['t_eff'], player_data_stats_df.at[0,'t_eff_mean'], player_data_stats_df.at[0,'t_eff_stdev'])
      table_data[f'Area {area}'][4] = trans_obj_area['t_create_str']
      percent, _ = calculate_percentile(trans_obj_area['t_create'], player_data_stats_df.at[0,'t_create_mean'], player_data_stats_df.at[0,'t_create_stdev'])
      table_data[f'Area {area}'][5] = f"{1-percent:.0%}"
      table_data[f'Area {area}'][6] = str(trans_obj_area['tran_total_pts'])

  # Calculate metrics for 'No Area'
  no_area_df = ppr_df[ (ppr_df['att_src_zone_net'] != 1) & 
    (ppr_df['att_src_zone_net'] != 2) & 
    (ppr_df['att_src_zone_net'] != 3) & 
    (ppr_df['att_src_zone_net'] != 4) & 
    (ppr_df['att_src_zone_net'] != 5) 
    ]
  trans_obj_no_area = calc_trans_obj(no_area_df, disp_player, 'srv')
  if trans_obj_no_area['status']:
    table_data['No Area'][0] = trans_obj_no_area['tcr_str']
    _, table_data['No Area'][1] = calculate_percentile(trans_obj_no_area['tcr'], player_data_stats_df.at[0,'tcr_mean'], player_data_stats_df.at[0,'tcr_stdev'])
    table_data['No Area'][2] = trans_obj_no_area['t_eff_str']
    _, table_data['No Area'][3] = calculate_percentile(trans_obj_no_area['t_eff'], player_data_stats_df.at[0,'t_eff_mean'], player_data_stats_df.at[0,'t_eff_stdev'])
    table_data['No Area'][4] = trans_obj_no_area['t_create_str']
    percent, _ = calculate_percentile(trans_obj_no_area['t_create'], player_data_stats_df.at[0,'t_create_mean'], player_data_stats_df.at[0,'t_create_stdev'])
    table_data['No Area'][5] = f"{1-percent:.0%}"
    table_data['No Area'][6] = str(trans_obj_no_area['tran_total_pts'])

  # Convert table_data to DataFrame
  df = pd.DataFrame(table_data)

  # Store the dataframe in df_list[0]
  df_list[0] = df.to_dict('records')
  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list



def report_player_trends(lgy, team, **rpt_filters):
  """
  Player trends report function - shows weekly performance trends for a player.
  
  Args:
    lgy: League+gender+year string
    team: Team identifier
    **rpt_filters: Additional report filters including optional start_date, end_date, player
    
  Returns:
    tuple: (title_list, label_list, image_list, df_list, df_desc_list, image_desc_list)
  """
  import anvil.tables as tables
  from anvil.tables import app_tables
  from datetime import datetime, timedelta
  import pandas as pd
  import matplotlib.pyplot as plt

  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe, and/or player stats, and/or tri-data
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)

  # Filter the ppr dataframe
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # This is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'] == disp_player) | 
    (ppr_df['player_a2'] == disp_player) |
    (ppr_df['player_b1'] == disp_player) |
    (ppr_df['player_b2'] == disp_player) 
    ]

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # =============================================================================

  # Determine start and end dates
  if 'start_date' in rpt_filters and 'end_date' in rpt_filters:
    start_date = rpt_filters.get('start_date')
    end_date = rpt_filters.get('end_date')
  else:
    # Query Anvil data table for subscriptions
    try:
      # Find matching subscription record in Anvil table
      matching_subscription = app_tables.subscriptions.search(
        team=team,
        league=disp_league,
        gender=disp_gender,
        year=str(disp_year)
      )

      if matching_subscription and len(matching_subscription) > 0:
        start_date = matching_subscription[0]['start_date']
        end_date = matching_subscription[0]['end_date']
        print(f"Found subscription dates: {start_date} to {end_date}")
      else:
        print(f"No matching subscription found for team={team}, league={disp_league}, gender={disp_gender}, year={disp_year}")
        # Fallback to default dates if no matching subscription found
        start_date = datetime(2025, 2, 19)
        end_date = start_date + timedelta(days=7*11)
    except Exception as e:
      print(f"Error querying subscription table: {e}")
      # Fallback to default dates if query fails
      start_date = datetime(2025, 2, 19)
      end_date = start_date + timedelta(days=7*11)

  # Convert to datetime objects if they aren't already
  if isinstance(start_date, str):
    start_date = pd.to_datetime(start_date)
  if isinstance(end_date, str):
    end_date = pd.to_datetime(end_date)

  # Convert to date objects for consistency
  if hasattr(start_date, 'date'):
    start_date = start_date.date()
  if hasattr(end_date, 'date'):
    end_date = end_date.date()

  # Calculate number of weeks based on actual date range
  total_days = (end_date - start_date).days
  num_weeks = int(total_days / 7) + (1 if total_days % 7 > 0 else 0)

  # Create a list with the start and end date for each week
  weekly_dates = []
  week_labels = []

  for i in range(num_weeks):
    week_start = start_date + timedelta(days=7 * i)
    week_end = min(week_start + timedelta(days=6), end_date)  # Don't exceed end_date

    weekly_dates.append({
      'start_date': week_start,
      'end_date': week_end
    })

    # Create date labels for every week
    week_labels.append(week_start.strftime('%m/%d/%y'))

  # Set up the pandas dataframe with date labels
  df_dict = {
    'Variable': week_labels,
    'FBHE': [0] * num_weeks,
    'Errors': [0] * num_weeks,
    'Transition': [0] * num_weeks,
    'Knockout': [0] * num_weeks,
    'Good Pass': [0] * num_weeks,
    'Points': [0] * num_weeks
  }
  sum_df = pd.DataFrame.from_dict(df_dict)

  # Loop over the weeks to calculate metrics
  for i in range(num_weeks):
    # Filter ppr_df to tmp_df for this week
    tmp_df = ppr_df.copy()
    tmp_df['game_date'] = pd.to_datetime(tmp_df['game_date'])
    tmp_df['game_date'] = tmp_df['game_date'].dt.date

    tmp_df = tmp_df[ 
      (tmp_df['game_date'] >= weekly_dates[i]['start_date']) & 
      (tmp_df['game_date'] <= weekly_dates[i]['end_date']) 
      ]

    # Calculate player point totals for this week
    pt_totals_df = player_pt_total(tmp_df, disp_player)

    if pt_totals_df.shape[0] > 0:  # Check if there's data for this week
      # Calculate metrics with error handling for division by zero
      try:
        sum_df.loc[i,'FBHE'] = (pt_totals_df.at[0,'p_fbk']-pt_totals_df.at[0,'p_fbe']) / pt_totals_df.at[0,'p_att_total'] if pt_totals_df.at[0,'p_att_total'] > 0 else 0
        sum_df.loc[i,'Errors'] = (pt_totals_df.at[0,'p_fbe']+pt_totals_df.at[0,'p_tse']+pt_totals_df.at[0,'p_te_r']+pt_totals_df.at[0,'p_te_s']) / pt_totals_df.at[0,'pts_total'] if pt_totals_df.at[0,'pts_total'] > 0 else 0
        sum_df.loc[i,'Transition'] = (pt_totals_df.at[0,'p_tk_s']+pt_totals_df.at[0,'p_tk_r']+pt_totals_df.at[0,'o_te_r']+pt_totals_df.at[0,'o_te_s']) / pt_totals_df.at[0,'trans_total'] if pt_totals_df.at[0,'trans_total'] > 0 else 0
        sum_df.loc[i,'Knockout'] = (pt_totals_df.at[0,'p_tsa']+pt_totals_df.at[0,'o_bad_pass']) / pt_totals_df.at[0,'p_serves'] if pt_totals_df.at[0,'p_serves'] > 0 else 0
        sum_df.loc[i,'Good Pass'] = pt_totals_df.at[0,'p_good_pass'] / (pt_totals_df.at[0,'p_good_pass']+pt_totals_df.at[0,'p_bad_pass']) if (pt_totals_df.at[0,'p_good_pass']+pt_totals_df.at[0,'p_bad_pass']) > 0 else 0
        sum_df.loc[i,'Points'] = ((pt_totals_df.at[0,'p_tsa']+pt_totals_df.at[0,'p_fbk']+pt_totals_df.at[0,'p_tk_r']+pt_totals_df.at[0,'p_tk_s']) + (pt_totals_df.at[0,'o_tse']+pt_totals_df.at[0,'o_fbe']+pt_totals_df.at[0,'o_te_r']+pt_totals_df.at[0,'o_te_s'])) / pt_totals_df.at[0,'pts_total'] if pt_totals_df.at[0,'pts_total'] > 0 else 0
      except (KeyError, IndexError, ZeroDivisionError):
        # Set to 0 if calculation fails
        pass

    # Format the entries to 3 decimal places
    sum_df.loc[i,'FBHE'] = "{:.3f}".format(sum_df.loc[i,'FBHE'])
    sum_df.loc[i,'Errors'] = "{:.3f}".format(sum_df.loc[i,'Errors'])
    sum_df.loc[i,'Transition'] = "{:.3f}".format(sum_df.loc[i,'Transition'])
    sum_df.loc[i,'Knockout'] = "{:.3f}".format(sum_df.loc[i,'Knockout'])
    sum_df.loc[i,'Good Pass'] = "{:.3f}".format(sum_df.loc[i,'Good Pass'])
    sum_df.loc[i,'Points'] = "{:.3f}".format(sum_df.loc[i,'Points'])

  # Create bar graphs for each metric
  size = [11, 5]
  avg_title = disp_league + " Average : "

  # Generate plots with league averages where available
  try:
    plt1 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['FBHE'].tolist(), 'First Ball Hitting Efficiency', '', 'FBHE', size, avg_title, player_data_stats_df.at[0,'fbhe_mean'], False, '', '')
    plt2 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['Errors'].tolist(), 'Error Density', '', 'Error Density', size, avg_title, player_data_stats_df.at[0,'err_den_mean']/100, False, '', '')
    plt3 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['Transition'].tolist(), 'Transition Conversion', '', 'Transition Conversion', size, avg_title, player_data_stats_df.at[0,'tcr_mean']/100, False, '', '')
    plt4 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['Knockout'].tolist(), 'Serving Aggressiveness', '', 'Serving - Knockout Percent', size, avg_title, player_data_stats_df.at[0,'knockout_mean'], False, '', '')
    plt5 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['Good Pass'].tolist(), 'Passing Quality', '', 'Percent Good Passes', size, avg_title, player_data_stats_df.at[0,'goodpass_mean'], False, '', '')
    plt6 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['Points'].tolist(), 'Percent of Points Won', '', 'Percent of Points Earned', size, '', 0, False, '', '')
  except (KeyError, IndexError):
    # Fallback plots without league averages if stats not available
    plt1 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['FBHE'].tolist(), 'First Ball Hitting Efficiency', '', 'FBHE', size, '', 0, False, '', '')
    plt2 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['Errors'].tolist(), 'Error Density', '', 'Error Density', size, '', 0, False, '', '')
    plt3 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['Transition'].tolist(), 'Transition Conversion', '', 'Transition Conversion', size, '', 0, False, '', '')
    plt4 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['Knockout'].tolist(), 'Serving Aggressiveness', '', 'Serving - Knockout Percent', size, '', 0, False, '', '')
    plt5 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['Good Pass'].tolist(), 'Passing Quality', '', 'Percent Good Passes', size, '', 0, False, '', '')
    plt6 = plot_bar_graph(sum_df['Variable'].tolist(), sum_df['Points'].tolist(), 'Percent of Points Won', '', 'Percent of Points Earned', size, '', 0, False, '', '')

  # Store the images in the list
  image_list[0] = plt1
  image_list[1] = plt2
  image_list[2] = plt3
  image_list[3] = plt4
  image_list[4] = plt5
  image_list[5] = plt6

  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list


def report_player_profile(lgy, team, **rpt_filters):
  """
  Player profile report function - displays player info and key metrics in a single dataframe.
  
  Args:
    lgy: League+gender+year string
    team: Team identifier
    **rpt_filters: Additional report filters including optional player
    
  Returns:
    tuple: (title_list, label_list, image_list, df_list, df_desc_list, image_desc_list)
  """
  from scipy.stats import norm

  # Get basic title and label setup from database
  title_list, label_list, df_desc_list, image_desc_list = setup_report_basics(lgy, team)

  # Initialize the calculated lists
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # Unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # Fetch the ppr dataframe and player stats
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  player_data_df, player_data_stats_df = get_player_data(disp_league, disp_gender, disp_year)

  # Debug: Check player_data_stats_df
  print(f"player_data_stats_df shape: {player_data_stats_df.shape}")
  print(f"player_data_stats_df columns: {player_data_stats_df.columns.tolist()}")

  # Filter the ppr dataframe
  ppr_df = filter_ppr_df(ppr_df, **rpt_filters)

  # This is a player report, so limit the data to plays with this player
  disp_player = rpt_filters.get('player')
  ppr_df = ppr_df[ (ppr_df['player_a1'].str.strip() == disp_player.strip()) | 
    (ppr_df['player_a2'].str.strip() == disp_player.strip()) |
    (ppr_df['player_b1'].str.strip() == disp_player.strip()) |
    (ppr_df['player_b2'].str.strip() == disp_player.strip()) 
    ]

  # Debug: Print the size of ppr_df after filtering
  print(f"Size of ppr_df after filtering for player '{disp_player}': {ppr_df.shape[0]} rows")

  # =============================================================================
  # REPORT-SPECIFIC LOGIC STARTS HERE
  # =============================================================================

  # Fetch player info from master_player table using disp_player as 'team number shortname'
  player_info = {'Team': '', 'Number': '', 'Shortname': '', 'Fullname': ''}
  try:
    # Split disp_player into team, number, and shortname
    player_parts = disp_player.strip().split(' ', 2)
    if len(player_parts) != 3:
      print(f"Error: disp_player '{disp_player}' does not match expected format 'team number shortname'")
      raise ValueError("Invalid player format")

    team_part, number_part, shortname_part = player_parts
    print(f"Searching master_player with team='{team_part}', number='{number_part}', shortname='{shortname_part}'")

    # Query master_player table
    player_record = app_tables.master_player.search(
      team=q.ilike(team_part),
      number=q.ilike(number_part),
      shortname=q.ilike(shortname_part)
    )

    if player_record and len(player_record) > 0:
      player_info['Team'] = player_record[0]['team'] or ''
      player_info['Number'] = player_record[0]['number'] or ''
      player_info['Shortname'] = player_record[0]['shortname'] or ''
      player_info['Fullname'] = player_record[0]['fullname'] or ''
      print(f"Found player: {player_info}")
    else:
      print(f"No player found in master_player for team='{team_part}', number='{number_part}', shortname='{shortname_part}'")
  except Exception as e:
    print(f"Error querying master_player table for {disp_player}: {e}")

  # Initialize dictionary for dataframe
  metrics_dict = {
    'Metric': ['Player Info'] + ['FBHE', 'FBSO', 'TCR', 'Expected Value', 'Error Density', 'Knockout', 'Ace/Error', 'Goodpass', 'Cons_ed_sd_match'],
    'Value': [f"{player_info['Team']} #{player_info['Number']} {player_info['Shortname']} ({player_info['Fullname']})"] + ['0.000'] * 2 + ['0%'] * 6 + ['0.000'] * 1,
    'League Average': [''] + ['0.000'] * 2 + ['0%'] * 6 + ['0.000'] * 1,
    'League Percentile': [''] + ['0%'] * 9
  }

  # Debug: Check metrics_dict initialization
  #print(f"metrics_dict['Value'] length: {len(metrics_dict['Value'])}")
  #print(f"metrics_dict['Value'] contents: {metrics_dict['Value']}")

  # Calculate metrics with error handling
  if not ppr_df.empty:  # Check if there's data for this player
    try:
      # FBHE and FBSO from fbhe_obj
      fbhe_result = fbhe_obj(ppr_df, disp_player, 'att', False)
      #print(f"FBHE result: {fbhe_result.__dict__}")
      metrics_dict['Value'][1] = "{:.3f}".format(fbhe_result.fbhe)
      metrics_dict['Value'][2] = "{:.3f}".format(fbhe_result.fbso)
      metrics_dict['League Average'][1] = "{:.3f}".format(player_data_stats_df.at[0,'fbhe_mean'] if 'fbhe_mean' in player_data_stats_df.columns else 0)
      metrics_dict['League Average'][2] = "{:.3f}".format(player_data_stats_df.at[0,'fbso_mean'] if 'fbso_mean' in player_data_stats_df.columns else 0)
      # Calculate percentiles for FBHE and FBSO
      if 'fbhe_mean' in player_data_stats_df.columns and 'fbhe_stdev' in player_data_stats_df.columns:
        z_score = (fbhe_result.fbhe - player_data_stats_df.at[0,'fbhe_mean']) / player_data_stats_df.at[0,'fbhe_stdev'] if player_data_stats_df.at[0,'fbhe_stdev'] > 0 else 0
        metrics_dict['League Percentile'][1] = "{:.0%}".format(norm.cdf(z_score))
      if 'fbso_mean' in player_data_stats_df.columns and 'fbso_stdev' in player_data_stats_df.columns:
        z_score = (fbhe_result.fbso - player_data_stats_df.at[0,'fbso_mean']) / player_data_stats_df.at[0,'fbso_stdev'] if player_data_stats_df.at[0,'fbso_stdev'] > 0 else 0
        metrics_dict['League Percentile'][2] = "{:.0%}".format(norm.cdf(z_score))

      # TCR from calc_trans_obj
      trans_result = calc_trans_obj(ppr_df, disp_player, 'all')
      #print(f"TCR result: {trans_result}")
      metrics_dict['Value'][3] = "{:.0%}".format(trans_result['tcr'] if trans_result['tcr'] is not None else 0)
      metrics_dict['League Average'][3] = "{:.0%}".format(player_data_stats_df.at[0,'tcr_mean'] if 'tcr_mean' in player_data_stats_df.columns else 0)
      # Calculate percentile for TCR
      if 'tcr_mean' in player_data_stats_df.columns and 'tcr_stdev' in player_data_stats_df.columns:
        z_score = (trans_result['tcr'] - player_data_stats_df.at[0,'tcr_mean']) / player_data_stats_df.at[0,'tcr_stdev'] if player_data_stats_df.at[0,'tcr_stdev'] > 0 else 0
        metrics_dict['League Percentile'][3] = "{:.0%}".format(norm.cdf(z_score))

      # Expected Value from calc_ev_obj
      ev_result = calc_ev_obj(ppr_df, disp_player)
      #print(f"Expected Value result: {ev_result}")
      metrics_dict['Value'][4] = "{:.3f}".format(ev_result['expected_value'] if ev_result['total_points'] > 0 else 0)
      metrics_dict['League Average'][4] = "{:.3f}".format(player_data_stats_df.at[0,'expected_mean'] / 100 if 'expected_mean' in player_data_stats_df.columns else 0)
      # Calculate percentile for Expected Value (accounting for 100x scaling)
      if 'expected_mean' in player_data_stats_df.columns and 'expected_stdev' in player_data_stats_df.columns:
        z_score = (ev_result['expected_value'] - (player_data_stats_df.at[0,'expected_mean'] / 100)) / (player_data_stats_df.at[0,'expected_stdev'] / 100) if player_data_stats_df.at[0,'expected_stdev'] > 0 else 0
        metrics_dict['League Percentile'][4] = "{:.0%}".format(norm.cdf(z_score))

      # Error Density from calc_error_density_obj
      error_density_result = calc_error_density_obj(ppr_df, disp_player)
      #print(f"Error Density result: {error_density_result}")
      metrics_dict['Value'][5] = "{:.0%}".format(error_density_result['error_density_raw'] if error_density_result['total_points'] > 0 else 0)
      metrics_dict['League Average'][5] = "{:.0%}".format(player_data_stats_df.at[0,'err_den_mean'] / 100 if 'err_den_mean' in player_data_stats_df.columns else 0)
      # Calculate percentile for Error Density
      if 'err_den_mean' in player_data_stats_df.columns and 'err_den_stdev' in player_data_stats_df.columns:
        z_score = (error_density_result['error_density_raw'] - (player_data_stats_df.at[0,'err_den_mean'] / 100)) / (player_data_stats_df.at[0,'err_den_stdev'] / 100) if player_data_stats_df.at[0,'err_den_stdev'] > 0 else 0
        metrics_dict['League Percentile'][5] = "{:.0%}".format(norm.cdf(z_score))

      # Knockout from calc_knock_out_obj
      knockout_result = calc_knock_out_obj(ppr_df, disp_player)
      #print(f"Knockout result: {knockout_result}")
      metrics_dict['Value'][6] = "{:.0%}".format(knockout_result['knock_out_rate'] if knockout_result['has_serves'] else 0)
      metrics_dict['League Average'][6] = "{:.0%}".format(player_data_stats_df.at[0,'knockout_mean'] if 'knockout_mean' in player_data_stats_df.columns else 0)
      # Calculate percentile for Knockout
      if 'knockout_mean' in player_data_stats_df.columns and 'knockout_stdev' in player_data_stats_df.columns:
        z_score = (knockout_result['knock_out_rate'] - player_data_stats_df.at[0,'knockout_mean']) / player_data_stats_df.at[0,'knockout_stdev'] if player_data_stats_df.at[0,'knockout_stdev'] > 0 else 0
        metrics_dict['League Percentile'][6] = "{:.0%}".format(norm.cdf(z_score))

      # Ace/Error from calc_ace_error_ratio
      ace_error = calc_ace_error_ratio(ppr_df, disp_player)
      #print(f"Ace/Error result: {ace_error}")
      metrics_dict['Value'][7] = "{:.0%}".format(ace_error if ace_error != float('inf') else 0)
      metrics_dict['League Average'][7] = "{:.0%}".format(player_data_stats_df.at[0,'ace_error_mean'] if 'ace_error_mean' in player_data_stats_df.columns else 0)
      # Calculate percentile for Ace/Error
      if 'ace_error_mean' in player_data_stats_df.columns and 'ace_error_stdev' in player_data_stats_df.columns:
        z_score = (ace_error - player_data_stats_df.at[0,'ace_error_mean']) / player_data_stats_df.at[0,'ace_error_stdev'] if player_data_stats_df.at[0,'ace_error_stdev'] > 0 and ace_error != float('inf') else 0
        metrics_dict['League Percentile'][7] = "{:.0%}".format(norm.cdf(z_score))

      # Goodpass from count_good_passes_obj
      goodpass_result = count_good_passes_obj(ppr_df, disp_player, 'pass')
      #print(f"Goodpass result: {goodpass_result}")
      metrics_dict['Value'][8] = "{:.0%}".format(goodpass_result['percent'] if goodpass_result['attempts'] > 0 else 0)
      metrics_dict['League Average'][8] = "{:.0%}".format(player_data_stats_df.at[0,'goodpass_mean'] if 'goodpass_mean' in player_data_stats_df.columns else 0)
      # Calculate percentile for Goodpass
      if 'goodpass_mean' in player_data_stats_df.columns and 'goodpass_stdev' in player_data_stats_df.columns:
        z_score = (goodpass_result['percent'] - player_data_stats_df.at[0,'goodpass_mean']) / player_data_stats_df.at[0,'goodpass_stdev'] if player_data_stats_df.at[0,'goodpass_stdev'] > 0 else 0
        metrics_dict['League Percentile'][8] = "{:.0%}".format(norm.cdf(z_score))

      # Cons_ed_sd_match from player_data_df
      cons_ed_sd = player_data_df[player_data_df['player'] == disp_player]['cons_ed_sd_match'].iloc[0] if not player_data_df[player_data_df['player'] == disp_player].empty and 'cons_ed_sd_match' in player_data_df.columns else 0
      #print(f"Cons_ed_sd_match value: {cons_ed_sd}")
      # Debug: Check metrics_dict['Value'] before assignment
      #print(f"Before Cons_ed_sd_match assignment - metrics_dict['Value'] length: {len(metrics_dict['Value'])}")
      #print(f"Before Cons_ed_sd_match assignment - metrics_dict['Value'] contents: {metrics_dict['Value']}")
      metrics_dict['Value'][9] = "{:.3f}".format(cons_ed_sd)
      metrics_dict['League Average'][9] = "{:.3f}".format(player_data_stats_df.at[0,'cons_ed_sd_match_mean'] if 'cons_ed_sd_match_mean' in player_data_stats_df.columns else 0)
      # Calculate percentile for Cons_ed_sd_match
      try:
        if 'cons_ed_sd_match_mean' in player_data_stats_df.columns and 'cons_ed_sd_match_stdev' in player_data_stats_df.columns:
          z_score = (cons_ed_sd - player_data_stats_df.at[0,'cons_ed_sd_match_mean']) / player_data_stats_df.at[0,'cons_ed_sd_match_stdev'] if player_data_stats_df.at[0,'cons_ed_sd_match_stdev'] > 0 else 0
          metrics_dict['League Percentile'][9] = "{:.0%}".format(norm.cdf(z_score))
        else:
          print(f"Warning: Missing cons_ed_sd_match_mean or cons_ed_sd_match_stdev in player_data_stats_df")
          metrics_dict['League Percentile'][9] = "{:.0%}".format(0)
      except Exception as e:
        print(f"Error in Cons_ed_sd_match percentile calculation: {e}")
        metrics_dict['League Percentile'][9] = "{:.0%}".format(0)

    except Exception as e:
      print(f"Error calculating metrics for {disp_player}: {e}")
      # Set to 0 if calculation fails
      for i in range(1, 3):
        metrics_dict['Value'][i] = "{:.3f}".format(0)
        metrics_dict['League Average'][i] = "{:.3f}".format(0)
        metrics_dict['League Percentile'][i] = "{:.0%}".format(0)
      for i in range(3, 9):
        metrics_dict['Value'][i] = "{:.0%}".format(0)
        metrics_dict['League Average'][i] = "{:.0%}".format(0)
        metrics_dict['League Percentile'][i] = "{:.0%}".format(0)
      metrics_dict['Value'][9] = "{:.3f}".format(0)
      metrics_dict['League Average'][9] = "{:.3f}".format(0)
      metrics_dict['League Percentile'][9] = "{:.0%}".format(0)

  # Create dataframe from metrics
  sum_df = pd.DataFrame.from_dict(metrics_dict)

  # Store the dataframe as a list of dicts in df_list
  df_list[0] = sum_df.to_dict('records')
  df_desc_list[0] = f"Player profile for {disp_player} in {disp_league} {disp_gender} {disp_year}"

  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list
  