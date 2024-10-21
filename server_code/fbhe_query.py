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
from tabulate import tabulate
from server_functions import *

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

@anvil.server.callable
def fbhe_table_query(disp_league, disp_gender, disp_year, disp_team, disp_player):
  # return a fbe of a given player
  # we will return the text as markdown to be displayed in a rich text box in the client

  # double checked that they are logged in

  # create the output dataframe
  df_dict = {' ':['FBHE','Kills','Errors','Attempts', ' ','URL'],
             'All':[0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,' '],
             "Zone 2":[0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,' '],
             'Zone 4':[0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  #print(fbhe_table)
  
  # filter the master_ppr_data file to this player and team and league)
  print(f"FBHE_table_query: League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}, Player:{disp_player}")
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = disp_team
      ) )

  if ppr_csv_row:
    m_ppr_df =  pd.read_csv(io.BytesIO( ppr_csv_row['ppr_csv'].get_bytes()))
  else:
    print('No Rows Found')
  
  if ppr_csv_row and m_ppr_df.shape[0] != 0:
    # calculate fbhe for all attacks
    fbhe_vector = fbhe( m_ppr_df, disp_player )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['att_src_zone_net']==i], disp_player )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    fbhe_return = pd.DataFrame.to_markdown(fbhe_table)
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return

@anvil.server.callable
def fbhe_scout_query(disp_league, disp_gender, disp_year, 
                    disp_team, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout
                    ):
  
  # return a fbhe of a given player
  # we will return the text as markdown to be displayed in a rich text box in the client
  # the difference here is that we combine the scout file with the team file to query aginst.
  # double checked that they are logged in
  # scout should always be true here

  # create the output dataframe
  df_dict = {' ':['FBHE','Kills','Errors','Attempts', ' ','URL'],
             'All':[0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,' '],
             "Zone 2":[0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,' '],
             'Zone 4':[0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  # get the ppr data
  m_ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True )

  # now, narrow the data by competition level and dates
  m_ppr_df = ppr_df_limit( m_ppr_df, 
                          comp_l1_checked, disp_comp_l1, 
                          comp_l2_checked, disp_comp_l2, 
                          comp_l3_checked, disp_comp_l3, 
                          date_checked, disp_start_date, disp_end_date
                         )
    
  print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  # if the eata is not empty, create my df, populate it, and return it
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['att_src_zone_net']==i], disp_player )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    fbhe_return = pd.DataFrame.to_markdown(fbhe_table)
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return

@anvil.server.callable
def fbhe_by_tactic(disp_league, disp_gender, disp_year, 
                    disp_team, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout
               ):
  # return a markdown text to display
  # given the parameters

  ############## First - Get the Data, and limit it by the parameters - Generaic for all reports
  m_ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, scout )
  m_ppr_df = ppr_df_limit( m_ppr_df, 
                          comp_l1_checked, disp_comp_l1, 
                          comp_l2_checked, disp_comp_l2, 
                          comp_l3_checked, disp_comp_l3, 
                          date_checked, disp_start_date, disp_end_date
                         )
    
  print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  # create the output dataframe - This is speficif to the report
  df_dict = {' ':['FBHE','Kills','Errors','Attempts', ' ','URL'],
             'All':[0,0,0,0,0,' '],
             'Option':[0,0,0,0,0,' '],
             "Behind":[0,0,0,0,0,' '],
             'Tempo':[0,0,0,0,0,' '],
             'Push to Pin':[0,0,0,0,0,' '],
             'Other':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # Option
    tmp_ppr_df = m_ppr_df[m_ppr_df['tactic'] == "option"]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player )
    fbhe_table.at[0,'Option'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Option'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Option'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Option'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Option'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Option'] = fbhe_vector[5]  # URL

    # Behind
    tmp_ppr_df = m_ppr_df[m_ppr_df['tactic'] == "behind"]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player )
    fbhe_table.at[0,'Behind'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Behind'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Behind'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Behind'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Behind'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Behind'] = fbhe_vector[5]  # URL

    # Tempo
    tmp_ppr_df = m_ppr_df[(m_ppr_df['set_height'] <= 2) & (m_ppr_df['set_dist'] <= 1) ]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player )
    fbhe_table.at[0,'Tempo'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Tempo'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Tempo'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Tempo'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Tempo'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Tempo'] = fbhe_vector[5]  # URL

    # Push to Pin
    tmp_ppr_df = m_ppr_df[ ( (m_ppr_df['att_src_zone_net'] == 1) & (m_ppr_df['set_src_zone_net'] == 4) | 
                           (m_ppr_df['att_src_zone_net'] == 5) & (m_ppr_df['set_src_zone_net'] == 2) ) &
                            ( m_ppr_df['set_height'] < 2 )
                          ]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player )
    fbhe_table.at[0,'Push to Pin'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Push to Pin'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Push to Pin'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Push to Pin'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Push to Pin'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Push to Pin'] = fbhe_vector[5]  # URL

    # Other
    tmp_ppr_df = m_ppr_df[  (m_ppr_df['tactic'] != 'behind' ) & 
                            (m_ppr_df['tactic'] != 'option' ) &
                            ~( ((m_ppr_df['att_src_zone_net'] == 1) & (m_ppr_df['set_src_zone_net'] == 4) | 
                               (m_ppr_df['att_src_zone_net'] == 5) & (m_ppr_df['set_src_zone_net'] == 2) ) &
                               ( m_ppr_df['set_height'] < 2 )) & 
                            ~ ((m_ppr_df['set_height'] <= 2) & (m_ppr_df['set_dist'] <= 1))
                          ]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player )
    fbhe_table.at[0,'Other'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Other'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Other'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Other'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Other'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Other'] = fbhe_vector[5]  # URL
    
    # Now, convert to a markdown file
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table)
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return

@anvil.server.callable
def fbhe_by_srv_src(disp_league, disp_gender, disp_year, 
                    disp_team, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout
               ):
  # return a markdown text to display
  # given the parameters

  ############## First - Get the Data, and limit it by the parameters - Generaic for all reports
  m_ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, scout )
  m_ppr_df = ppr_df_limit( m_ppr_df, 
                          comp_l1_checked, disp_comp_l1, 
                          comp_l2_checked, disp_comp_l2, 
                          comp_l3_checked, disp_comp_l3, 
                          date_checked, disp_start_date, disp_end_date
                         )
    
  print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  # create the output dataframe - This is speficif to the report
  df_dict = {' ':['FBHE','Kills','Errors','Attempts', ' ','URL'],
             'All':[0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,' '],
             "Zone 2":[0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,' '],
             'Zone 4':[0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['serve_src_zone_net']==i], disp_player )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table)
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return

@anvil.server.callable
def report_stub(disp_league, disp_gender, disp_year, 
                    disp_team, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout
               ):
  # return a markdown text to display
  # given the parameters

  ############## First - Get the Data, and limit it by the parameters - Generaic for all reports
  m_ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, scout )
  m_ppr_df = ppr_df_limit( m_ppr_df, 
                          comp_l1_checked, disp_comp_l1, 
                          comp_l2_checked, disp_comp_l2, 
                          comp_l3_checked, disp_comp_l3, 
                          date_checked, disp_start_date, disp_end_date
                         )
    
  print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  # create the output dataframe - This is speficif to the report
  df_dict = {' ':['FBHE','Kills','Errors','Attempts', ' ','URL'],
             'All':[0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,' '],
             "Zone 2":[0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,' '],
             'Zone 4':[0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['att_src_zone_net']==i], disp_player )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table)
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return