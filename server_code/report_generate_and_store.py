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
import datetime
from server_functions import *
import inspect
import matplotlib.pyplot as plt
import matplotlib as mpl
import math
from plot_functions import *
import numpy as np
from server_functions import *

@anvil.server.callable
def generate_and_store_report( fnct_name, lgy, team, **rpt_filters ):
  #
  # this routine runs the given report (fnct_name) using the parameters in *(**kwargs) and 
  # stores the data in the report_data table, returngin the index to that table
  #

  #
  # Call the report function storing the results in three lists:
  #      label_list[ ] - list of text tables
  #.     image_list[] - list of images, mainly plots
  #.     df_list[] - a list of dataframes (to be converted to mkdn), but kep as a df for now
  #
  #.     Th emax we can store are 10 items in each list, that is the max in the report_data tables, and the hmtl forms are configured for a max of 10 in each
  title_list = []
  label_list = []
  image_list = []
  df_list = []

  title_list, label_list, image_list, df_list = anvil.server.call( fnct_name, lgy, team, **rpt_filters )

  #print(f"Title List returned, Length: {len(title_list)}, list: {title_list}")
  #print(f"Label List returned, Length: {len(label_list)}, list: {label_list}")
  #print(f"Image List returned, Length: {len(image_list)}, list: {image_list}")
  #print(f"DF List returned, Length: {len(df_list)}, list: {df_list}")
  
  # now store the returned data in the report_data table
  # Generate unique report ID
  report_id = str(uuid.uuid4())

  # Store in temporary Data Table row
  app_tables.report_data.add_row(
    report_id=report_id,
    created_at=datetime.datetime.now()
  )

  # get this record and store the labels , images, and dfs
  rpt_data_row = app_tables.report_data.get(report_id = report_id)

  rpt_data_row['no_title'] = len(title_list)  
  if len(title_list) > 0:
    for i in range(0,len(title_list)):
      var = 'title_'+str(i+1)
      #print(f" label to be stored: {var}, title: {title_list[i]}")
      rpt_data_row[var] = title_list[i]
      
  rpt_data_row['no_label'] = len(label_list)
  if len(label_list) > 0:
    for i in range(0,len(label_list)):
      var = 'label_'+str(i+1)
      #print(f" label to be stored: {var}, label: {label_list[i]}")
      rpt_data_row[var] = label_list[i]
      
  no_images = 0
  if len(image_list) > 0:
    for i in range(0,len(image_list)):
      if type(image_list[i]) is not str:
        var = 'image_'+str(i+1)
        rpt_data_row[var] = image_list[i]
        no_images = no_images + 1
  rpt_data_row['no_image'] = no_images 
      
  no_dfs = 0
  #print(f"store report data, df_list, length {len(df_list)} df list : {df_list}")
  if len(df_list) > 0:
    for i in range(0,len(df_list)):
      print(f"df list # :{i}, df : {type(df_list[i])}")
      var = 'df_'+str(i+1)
      if len(df_list[i]) != 0:

        # Validate and recover DataFrame
        if not isinstance(df_list[i], list) or not all(isinstance(row, dict) for row in df_list[i]):
          raise ValueError(f"Expected list of dictionaries, got {type(df_list[i])}")
        try:
          df_tmp = pd.DataFrame(df_list[i])
        except Exception as e:
          raise ValueError(f"Failed to recover DataFrame: {str(e)}")

        # need to store the data as a markdown, not datafarme (no pandas in the client)
        mkdn_file = pd.DataFrame.to_markdown(df_tmp, index = False ) # now convert it to mkdn formate
        rpt_data_row[var] = anvil.BlobMedia(content_type="text/plain", content=mkdn_file.encode(), name=var+'.mkdn')
        no_dfs = no_dfs + 1
  rpt_data_row['no_df'] = no_dfs

  return report_id

@anvil.server.callable
def get_report_data(report_id):

  row = app_tables.report_data.get(report_id=report_id)

  title_list = []
  label_list = []
  image_list = []
  df_list = []
  
  if not row:
    return None
  else:
    for i in range(0,row['no_title']):
      title_var = 'title_'+str(i+1)
      title_list.append(row[title_var])
      
    for i in range(0,row['no_label']):
      label_var = 'label_'+str(i+1)
      label_list.append(row[label_var])
      
    for i in range(0,row['no_image']):
      image_var = 'image_'+str(i+1)
      image_list.append(row[image_var])

    for i in range(0,row['no_df']):
      df_var = 'df_'+str(i+1)
      mkdn_file = row[df_var].get_bytes().decode('utf-8')
      df_list.append(mkdn_file)

        
  return title_list, label_list, image_list, df_list

@anvil.server.callable
def report_test( lgy, team, **rpt_filters):
  # lgy is the legaue+gender+year string
  # unpack lgy into league, gender, year
  disp_league, disp_gender, disp_year = unpack_lgy(lgy)

  # fetch the ppr dataframe
  ppr_df = get_ppr_data(disp_league, disp_gender, disp_year, team, True)
  
  # now filter the ppr dataframe
  ppr_df = filter_ppr_df( ppr_df, **rpt_filters)
    
  # initiate return lists
  title_list = ['','','','','','','','','','']
  label_list = ['','','','','','','','','','']
  image_list = ['','','','','','','','','','']
  df_list = ['','','','','','','','','','']

  # fetch the labels from the database
  rpt_row = app_tables.report_list.get(function_name='report_test')
  title_list[0] = rpt_row['rpt_title']
  title_list[1] = rpt_row['rpt_sub_title']
  title_list[2] = rpt_row['rpt_section_title1']
  title_list[3] = rpt_row['rpt_section_title2']
  title_list[4] = rpt_row['team_name']
  title_list[5] = rpt_row['company_name']
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
  
  
  return title_list, label_list, image_list, df_list

@anvil.server.callable
def filter_ppr_df( dataframe, **kwargs):
  # given the dataframe, filter it by rpt_filters
  """
    Generate a report by filtering the DataFrame based on kwargs.
    Args:
        dataframe: pandas DataFrame to filter
        **kwargs: Keyword arguments where key=column_name, value=filter_value
    Returns:
        Filtered DataFrame

    This function DOES NOT filter on a given pair or player.  This needs to be done by the report function

    This function filters on:
      comp_l1
      comp_l2
      comp_l3
      opp_pair
      start_date, end_date
      set 1,2,3
      srv_fr [1,3,5]
      srv_to [1e ... 5c]
      serve speed low & high
      pass out of system
      pass height low & high
      set height low & high
      set touch type (bump, hand, unknown)
      att_height low & high
      att_speed low & high
    """
  result = dataframe.copy()  # Avoid modifying the original DataFrame
  for column, value in kwargs.items():
    #if column in dataframe.columns:
     # we'll check for ones not set up as equal to
    # this includes: Date Range, srv_to, srv_fr, srv speed, pass height, set height, att speed, att height
      
    if column == 'comp_l1':
      result = result[ result['comp_l1'] == value ]
    if column == 'comp_l2':
      result = result[ result['comp_l2'] == value ]
    if column == 'comp_l3':
      result = result[ result['comp_l3'] == value ]
      
    if column == 'start_date':
      result = result[ result['date'] >= value ]
    if column == 'end_date':
      result = result[ result['date'] <= value ]

    # opponent pair for matchu0ps
    if column == 'opp_pair':
      result = result[ ( result['teama'] == value | result['teamb'] == value ) ]

    # set, these are setup as a radio button, so only one can be chceked.  We pass the 'set' as either 1,2,3, or it is not in the list
    if column == 'set':
      result = result[ result[column] == value ]

    # serving to, this is a list of up to 15 zones
    if column == 'srv_fr':
      result = result[ result['serve_src_zone_net'].isin(value) ]
      
    # serving from, this is a list of 3 zones
    if column == 'srv_to':
      result['srv_to'] = str(result['serve_dest_zone_net'])+result['serve_dest_zone_depth']
      result = result[ result['srv_to'].isin(value) ]
  
    # serve speed
    if column == 'srv_speed_low':
      result = result[ result['srv_speed'] >= value ]
    if column == 'srv_speed_high':
      result = result[ result['srv_speed'] <= value ]

    # pass out of system
    if column == 'pass_oos':
      if value == 0:
        result = result[ result['pass_height'] == value ]
      else:
        result = result[ result['pass_height'] >= 1 ]

    # pass height
    if column == 'pass_ht_low':
      result = result[ result['pass_height'] >= value ]
    if column == 'pass_ht_high':
      result = result[ result['pass_height'] <= value ]
         
    # set height
    if column == 'set_ht_low':
      result = result[ result['set_height'] >= value ]
    if column == 'set_ht_high':
      result = result[ result['set_height'] <= value ]
        
    # set type : bump, hand, unknownn, where only one can be selected
    if column == 'set_touch_type':
      if value == ' unkown':
        result = result[ ( result[column] == value | result[column] == 'empty' ) ]
      else:
        result = result[ result[column] == value ]
         
    # att height
    if column == 'att_ht_low':
      result = result[ result['att_height'] >= value ]
    if column == 'att_ht_high':
      result = result[ result['att_height'] <= value ]
        
    # att speed
    if column == 'att_speed_low':
      result = result[ result['att_speed'] >= value ]
    if column == 'att_speed_high':
      result = result[ result['att_speed'] <= value ]
        
        
    #result = result[result[column] == value]
    print(f"Number of Rows: {result.shape[0]}")
    
    #else:
      #print(f"Warning: Column '{column}' not found in DataFrame")
    
  return result

@anvil.server.callable
def unpack_lgy(lgy):
  # unpacks the league/year/gender string into three
  # extract league, gender, year from league selected value
  str_loc = lgy.index("|")
  disp_league = lgy[: str_loc - 1].strip()
  lgy = lgy[str_loc + 1 :]
  str_loc = lgy.index("|")
  disp_gender = lgy[: str_loc - 1].strip()
  disp_year = lgy[str_loc + 1 :].strip()

  return disp_league, disp_gender, disp_year

@anvil.server.callable
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


@anvil.server.callable
def get_player_attack_table(ppr_df, disp_player):
  '''
  geenrates the dataframe for the player attacking table

  INPUTS:
    - ppr dataframe, limited as desired before calling
    - player for the dataframe

  RETURNS
    - the dataframe
    
  '''
  # create the output dataframe
  df_dict = {' ':['FBHE','FBSO','Kills','Errors','Attempts','URL'],
             'All':[0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,' '],
             "Zone 2":[0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,' '],
             'Zone 4':[0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,' '],
             'No Zone':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  # if the eata is not empty, create my df, populate it, and return it
  if ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( ppr_df, disp_player, 'att', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[2,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[3,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[4,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[1,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5','No Zone']
    for i in [1,2,3,4,5,6]:
      zone = 0 if i == 6 else i
      fbhe_vector = fbhe( ppr_df[ppr_df['att_src_zone_net']==zone], disp_player, 'att', True )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[2,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[3,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[4,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[1,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    return fbhe_table
  else:
    return "No Data Found"


@anvil.server.callable
def get_player_attack_plots( ppr_df, disp_player):
  
  # limit the data to passes by the player
  new_ppr = ppr_df[ ppr_df['att_player'] == disp_player]
  
  # set up 5 sub plots
  attack_z1_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 1) & (new_ppr['tactic'] != 'option')],'att',1)  
  attack_z2_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 2) & (new_ppr['tactic'] != 'option')],'att',2)
  attack_z3_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 3) & (new_ppr['tactic'] != 'option')],'att',3)
  attack_z4_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 4) & (new_ppr['tactic'] != 'option')],'att',4)
  attack_z5_plot_object = plot_lines_on_court(new_ppr[ (new_ppr['att_src_zone_net'] == 5) & (new_ppr['tactic'] != 'option')],'att',5)

  zone_dict = {'1':['FBHE','FBSO','ATT','URL'],'Value':[0,0,0,'']}
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

  return attack_z1_plot_object, attack_z2_plot_object, attack_z3_plot_object, attack_z4_plot_object, attack_z5_plot_object, z1_df, z2_df, z3_df, z4_df, z5_df
