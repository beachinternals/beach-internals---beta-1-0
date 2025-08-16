import anvil.server
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables.query as q
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from PIL import Image
from io import BytesIO
import base64
import pandas as pd
import uuid
import json
from datetime import datetime, timedelta, date
import inspect
import matplotlib.pyplot as plt
import matplotlib as mpl
import math
import scipy.stats as stats
import numpy as np
import importlib
import sys
from typing import Tuple, List, Dict, Any

from plot_functions import *
from server_functions import *
from reports_player_new import *

# Option A: Check if it's just called Logger
from anvil_extras.logging import Logger
# Create a detailed formatter
formatter = logging.Formatter(
  '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s() - %(message)s'
)
logger = Logger()




# Use it in your code
logger.info("Application started")
logger.debug("Debug information")
logger.warning("This is a warning")
logger.error("An error occurred")


# Cache for valid function names
_valid_functions_cache = None

def get_valid_functions() -> set:
  """Retrieve valid function names from the report_list table, with caching."""
  global _valid_functions_cache
  if _valid_functions_cache is None:
    _valid_functions_cache = {row['function_name'] for row in tables.app_tables.report_list.search()}
  return _valid_functions_cache

  
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
#
#        Generate and Store Report
#
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
@anvil.server.callable
def generate_and_store_report(fnct_name: str, lgy: str, team: str, **rpt_filters) -> str:
  """
    Generate a report using the specified function, store results in report_data table, and return the report ID.
    
    Args:
        fnct_name: Name of the report function to call.
        lgy: League identifier.
        team: Team identifier.
        **rpt_filters: Additional report filters.
    
    Returns:
        report_id: Unique ID of the stored report.
    
    Raises:
        ValueError: If function name is invalid or data processing fails.
    """
  # Initialize result lists
  title_list: List[str] = []
  label_list: List[str] = []
  image_list: List[Any] = []
  df_list: List[Dict] = []
  df_desc_list: List[str] = []
  image_desc_list: List[str] = []

  # Log the request
  logger.info(f"Generating report: fnct_name: {fnct_name}, lgy: {lgy}, team: {team}, filters: {rpt_filters}")

  # Validate function name against report_list table
  valid_functions = get_valid_functions()
  if fnct_name not in valid_functions:
    logger.error(f"Function name '{fnct_name}' not found in report_list table")
    title_list.append(f"Report Not Found: {fnct_name}")
  else:
    # Dynamically get the function from globals
    try:
      func = globals()[fnct_name]
    except KeyError:
      logger.error(f"Function '{fnct_name}' is not implemented in the global namespace")
      title_list.append(f"Function Error: {fnct_name}")
    else:
      # Call the function
      try:
        title_list, label_list, image_list, df_list, df_desc_list, image_desc_list = func(lgy, team, **rpt_filters)
      except Exception as e:
        logger.error(f"Error executing {fnct_name}: {str(e)}")
        title_list.append(f"Execution Error: {fnct_name}")

    # Close any open matplotlib plots
  try:
    import matplotlib.pyplot as plt
    plt.close('all')
  except ImportError:
    pass  # Matplotlib may not be used

    # Generate unique report ID
  report_id = str(uuid.uuid4())

  # Store in report_data table
  rpt_data_row = tables.app_tables.report_data.add_row(
    report_id=report_id,
    created_at=datetime.now()
  )

  # Store titles (max 10)
  rpt_data_row['no_title'] = min(len(title_list), 10)
  for i, title in enumerate(title_list[:10]):
    rpt_data_row[f'title_{i+1}'] = title

    # Store league in title_4
  rpt_data_row['title_4'] = lgy

  # Store filter text in title_7
  try:
    rpt_data_row['title_7'] = make_filter_text(lgy, **rpt_filters)
  except NameError:
    logger.warning("make_filter_text not defined; skipping filter text storage")
    rpt_data_row['title_7'] = str(rpt_filters)

  # Store player in title_9
  rpt_data_row['title_9'] = rpt_filters.get('player')

  # Store pair in title_10
  rpt_data_row['title_10'] = rpt_filters.get('pair')

    # Store labels (max 10)
  rpt_data_row['no_label'] = min(len(label_list), 10)
  for i, label in enumerate(label_list[:10]):
    rpt_data_row[f'label_{i+1}'] = label

    # Store images (max 10, only non-string types)
  no_images = 0
  for i, image in enumerate(image_list[:10]):
    if not isinstance(image, str):
      rpt_data_row[f'image_{i+1}'] = image
      no_images += 1
  rpt_data_row['no_image'] = no_images

  # Store image descriptions (max 10)
  rpt_data_row['no_image_desc'] = min(len(image_desc_list), 10)
  for i, image_desc in enumerate(image_desc_list[:10]):
    rpt_data_row[f'image_desc_{i+1}'] = image_desc

  # Store DataFrames as markdown (max 10)
  no_dfs = 0
  for i, df_data in enumerate(df_list[:10]):
    if df_data:  # Check if not empty
      try:
        if not isinstance(df_data, list) or not all(isinstance(row, dict) for row in df_data):
          raise ValueError(f"Expected list of dictionaries for df_{i+1}, got {type(df_data)}")
        df_tmp = pd.DataFrame(df_data)
        mkdn_file = df_tmp.to_markdown(index=False)
        rpt_data_row[f'df_{i+1}'] = anvil.BlobMedia(
          content_type="text/plain",
          content=mkdn_file.encode(),
          name=f'df_{i+1}.mkdn'
        )
        no_dfs += 1
      except Exception as e:
        logger.error(f"Failed to store DataFrame {i+1}: {str(e)}")
  rpt_data_row['no_df'] = no_dfs

  # Store DataFrame descriptions (max 10)
  rpt_data_row['no_df_desc'] = min(len(df_desc_list), 10)
  for i, df_desc in enumerate(df_desc_list[:10]):
    rpt_data_row[f'df_desc_{i+1}'] = df_desc

  logger.info(f"Stored report with ID: {report_id}")
  return report_id

  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
  #
  #        Get Report Data
  #
  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
@anvil.server.callable
def get_report_data(report_id):

  row = app_tables.report_data.get(report_id=report_id)

  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']
  df_desc_list = ['','','','','','','','','','']
  image_desc_list = ['','','','','','','','','','']

  if not row:
    return None
  else:
    for i in range(0,row['no_title']):
      title_var = 'title_'+str(i+1)
      title_list[i] = row[title_var]

    for i in range(0,row['no_label']):
      label_var = 'label_'+str(i+1)
      label_list[i] = row[label_var]

    for i in range(0,row['no_image']):
      image_var = 'image_'+str(i+1)
      image_list[i] = row[image_var]

    for i in range(0,row['no_df']):
      df_var = 'df_'+str(i+1)
      mkdn_file = row[df_var].get_bytes().decode('utf-8')
      df_list[i] = mkdn_file

    for i in range(0,row['no_df_desc']):
      df_desc_var = 'df_desc_'+str(i+1)
      df_desc_list[i] = row[df_desc_var]

    for i in range(0,row['no_image_desc']):
      image_desc_var = 'image_desc_'+str(i+1)
      image_desc_list[i] = row[image_desc_var]

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list

  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
  #
  #        Setup Report Basics
  #
  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
def setup_report_basics(lgy, team, function_name=None):
  """
  Helper function to set up basic title and label lists from report_list table.
  This can be reused by all report functions.
  
  Args:
    function_name: Name of the function in report_list table
    lgy: League identifier
    team: Team identifier
    
  Returns:
    tuple: (title_list, label_list) pre-populated from database
  """

  if function_name is None:
    function_name = inspect.stack()[1].function
    
  # initiate return lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  df_desc_list = ['','','','','','','','','','']
  image_desc_list = ['','','','','','','','','','']

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name=function_name)
  if rpt_row:
    title_list[0] = rpt_row['rpt_title']
    title_list[1] = rpt_row['rpt_sub_title']
    title_list[2] = rpt_row['rpt_section_title1']
    title_list[3] = lgy  # Override with actual lgy parameter
    title_list[4] = team  # Override with actual team parameter
    title_list[5] = rpt_row['rpt_type']
    title_list[6] = rpt_row['filter_text']
    title_list[7] = rpt_row['explain_text']

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
    df_desc_list[6] = rpt_row['df_desc_7']
    df_desc_list[7] = rpt_row['df_desc_8']
    df_desc_list[8] = rpt_row['df_desc_9']
    df_desc_list[9] = rpt_row['df_desc_10']

    image_desc_list[0] = rpt_row['image_desc_1']
    image_desc_list[1] = rpt_row['image_desc_2']
    image_desc_list[2] = rpt_row['image_desc_3']
    image_desc_list[3] = rpt_row['image_desc_4']
    image_desc_list[4] = rpt_row['image_desc_5']
    image_desc_list[5] = rpt_row['image_desc_6']
    image_desc_list[6] = rpt_row['image_desc_7']
    image_desc_list[7] = rpt_row['image_desc_8']
    image_desc_list[8] = rpt_row['image_desc_9']
    image_desc_list[9] = rpt_row['image_desc_10']
  

  return title_list, label_list, df_desc_list, image_desc_list

  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
  #
  #        Generic Report Stub - Copy this as a starting Point
  #
  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=---=-=-=-=-=-=-=-=--=-=-=-=-
def report_test(lgy, team, **rpt_filters):
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

  # Example: Create a simple DataFrame from filtered data
  if not ppr_df.empty:
    # Convert first 10 rows to dict format for storage
    sample_data = ppr_df.head(10).to_dict('records')
    df_list[0] = sample_data

  # Example: Create a summary DataFrame
  if not ppr_df.empty and len(ppr_df) > 1:
    summary_data = [
      {'Metric': 'Total Records', 'Value': len(ppr_df)},
      {'Metric': 'Team', 'Value': team},
      {'Metric': 'League', 'Value': disp_league},
      {'Metric': 'Gender', 'Value': disp_gender},
      {'Metric': 'Year', 'Value': disp_year}
    ]
    df_list[1] = summary_data

  # Example: You would generate actual plots/images here
  # For now, just adding placeholder descriptions
  if not ppr_df.empty:
    _ = None
    # image_list[0] = your_generated_plot_here
    # image_list[1] = your_other_plot_here

  # =============================================================================
  # END REPORT-SPECIFIC LOGIC
  # =============================================================================

  return title_list, label_list, image_list, df_list, df_desc_list, image_desc_list




def report_player_attacking( lgy, team, **rpt_filters):
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


@anvil.server.callable
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
      fbhe_table.at[1,column[i-1]] = str('{:.0%}ile').format(fbhe_table.at[1,column[i-1]])

    return fbhe_table
  else:
    fbhe_table.at[0,'Zone 1'] = 'No Data Found'
    return fbhe_table


@anvil.server.callable
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


  

def make_filter_text( lgy, **rpt_filters):
  
  filter_text = f"""
Data Filters:
- Date Created : {datetime.today().strftime('%Y-%m-%d')}
- League / Gender / Year : {lgy}
    """
  filter_text = filter_text + '\n'

  filter_text = filter_text if 'filter_text' in locals() else ""
  if rpt_filters.get("pair") is not None and rpt_filters.get("pair") != "":
    filter_text += f"- Pair = {str(rpt_filters['pair'])}\n"
  if rpt_filters.get("player") is not None and rpt_filters.get("player") != "":
    filter_text += f"- Player = {str(rpt_filters['player'])}\n"
  if rpt_filters.get("comp_l1") is not None and rpt_filters.get("comp_l1") != "":
    filter_text += f"- Competition Level 1 = {str(rpt_filters['comp_l1'])}\n"
  if rpt_filters.get("comp_l2") is not None and rpt_filters.get("comp_l2") != "":
    filter_text += f"- Competition Level 2 = {str(rpt_filters['comp_l2'])}\n"
  if rpt_filters.get("comp_l3") is not None and rpt_filters.get("comp_l3") != "":
    filter_text += f"- Competition Level 3 = {str(rpt_filters['comp_l3'])}\n"
  if rpt_filters.get("start_date") is not None and rpt_filters.get("start_date") != "":
    filter_text += f"- Start Date = {str(rpt_filters['start_date'])}\n"
  if rpt_filters.get("end_date") is not None and rpt_filters.get("end_date") != "":
    filter_text += f"- End Date = {str(rpt_filters['end_date'])}\n"
  if rpt_filters.get("set") is not None and rpt_filters.get("set") != "":
    filter_text += f"- Set = {str(rpt_filters['set'])}\n"
  if rpt_filters.get("set_ht_low") is not None and rpt_filters.get("set_ht_low") != "":
    filter_text += f"- Set Height, Low  = {str(rpt_filters['set_ht_low'])}\n"
  if rpt_filters.get("set_ht_high") is not None and rpt_filters.get("set_ht_high") != "":
    filter_text += f"- Set Height, High  = {str(rpt_filters['set_ht_high'])}\n"
  if rpt_filters.get("set_touch_type") is not None and rpt_filters.get("set_touch_type") != "":
    filter_text += f"- Set Touch Type = {str(rpt_filters['set_touch_type'])}\n"
  if rpt_filters.get("pass_oos") is not None and rpt_filters.get("pass_oos") != "":
    filter_text += f"- Pass = {' Out System Only' if rpt_filters['pass_oos'] == 1 else ' In of System Only' }\n"
  if rpt_filters.get("att_ht_low") is not None and rpt_filters.get("att_ht_low") != "":
    filter_text += f"- Attack Height, Low  = {str(rpt_filters['att_ht_low'])}\n"
  if rpt_filters.get("att_ht_high") is not None and rpt_filters.get("att_ht_high") != "":
    filter_text += f"- Attack Height, High = {str(rpt_filters['att_ht_high'])}\n"
  if rpt_filters.get("att_speed_low") is not None and rpt_filters.get("att_speed_low") != "":
    filter_text += f"- Attack Speed, Low = {str(rpt_filters['att_speed_low'])}\n"
  if rpt_filters.get("att_speed_high") is not None and rpt_filters.get("att_speed_high") != "":
    filter_text += f"- Attack Speed, High = {str(rpt_filters['att_speed_high'])}\n"
  if rpt_filters.get("pass_ht_low") is not None and rpt_filters.get("pass_ht_low") != "":
    filter_text += f"- Pass Height, Low = {str(rpt_filters['pass_ht_low'])}\n"
  if rpt_filters.get("pass_ht_high") is not None and rpt_filters.get("pass_ht_high") != "":
    filter_text += f"- Pass Height, High = {str(rpt_filters['pass_ht_high'])}\n"
  if rpt_filters.get("srv_fr") is not None and rpt_filters.get("srv_fr") != "":
    filter_text += f"- Serve From (Zones) = {str(rpt_filters['srv_fr'])}\n"
  if rpt_filters.get("srv_to") is not None and rpt_filters.get("srv_to") != "":
    filter_text += f"- Serve to (Zones) = {str(rpt_filters['srv_to'])}\n"

  #print(f" Filter Text: \n {filter_text}")
  return filter_text