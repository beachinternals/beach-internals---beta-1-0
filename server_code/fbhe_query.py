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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['att_src_zone_net']==i], disp_player, 'att' )
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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['att_src_zone_net']==i], disp_player, 'att' )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    fbhe_return = pd.DataFrame.to_markdown(fbhe_table)
  else:
    fbhe_return = "No Data Found"

  # explain the report here
  fbhe_return4 = "Explain the Report Here"
  
  return fbhe_return, ' ', ' ', fbhe_return4

@anvil.server.callable
def fbhe_by_attack_tactic(disp_league, disp_gender, disp_year, 
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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # Option
    tmp_ppr_df = m_ppr_df[m_ppr_df['tactic'] == "option"]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'att' )
    fbhe_table.at[0,'Option'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Option'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Option'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Option'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Option'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Option'] = fbhe_vector[5]  # URL

    # Behind
    tmp_ppr_df = m_ppr_df[m_ppr_df['tactic'] == "behind"]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'att' )
    fbhe_table.at[0,'Behind'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Behind'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Behind'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Behind'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Behind'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Behind'] = fbhe_vector[5]  # URL

    # Tempo
    tmp_ppr_df = m_ppr_df[(m_ppr_df['set_height'] <= 2) & (m_ppr_df['set_dist'] <= 1) ]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'att' )
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
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'att' )
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
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'att' )
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
  # explain the report here
  fbhe_return4 = "Explain the Report Here"
  
  return fbhe_return, ' ', ' ', fbhe_return4

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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['serve_src_zone_net']==i], disp_player, 'att' )
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
  
  # explain the report here
  fbhe_return4 = "Explain the Report Here"
  
  return fbhe_return, ' ', ' ', fbhe_return4

@anvil.server.callable
def fbhe_by_attack_type(disp_league, disp_gender, disp_year, 
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
             'Poke/Roll':[0,0,0,0,0,' '],
             'Shot':[0,0,0,0,0,' '],
             'Bang/Hard':[0,0,0,0,0,' '],
             ' - ':[0,0,0,0,0,' '],
             ' -- ':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # for this calculation, elminate bad speed and distance data
    # if both are 0, or speed is negaitve
    #m_ppr_df = m_ppr_df[ ~( m_ppr_df['att_speed'] == 0 & m_ppr_df['att_dist'] == 0 ) ]
    m_ppr_df = m_ppr_df[ (m_ppr_df['att_speed'] > 0) ]
    
    # calculate for poke/roll, limit the data, and call fbhe
    fbhe_vector = fbhe( m_ppr_df[ (m_ppr_df['att_speed'] <= 2.5/15*m_ppr_df['att_dist']) ], disp_player, 'att' )
    fbhe_table.at[0,'Poke/Roll'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Poke/Roll'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Poke/Roll'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Poke/Roll'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Poke/Roll'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Poke/Roll'] = fbhe_vector[5]  # URL

    # calculate for shot, imit the data, and call fbhe
    fbhe_vector = fbhe( m_ppr_df[ ~(m_ppr_df['att_speed'] <= 2.5/15*m_ppr_df['att_dist']) & ( m_ppr_df['att_speed'] <= 6 ) ], disp_player, 'att' )
    fbhe_table.at[0,'Shot'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Shot'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Shot'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Shot'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Shot'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Shot'] = fbhe_vector[5]  # URL

        # calculate for poke/roll
    # limit the data, and call fbhe
    fbhe_vector = fbhe( m_ppr_df[ ~(m_ppr_df['att_speed'] <= 2.5/15*m_ppr_df['att_dist']) &  ( m_ppr_df['att_speed'] > 6 ) ], disp_player, 'att' )
    fbhe_table.at[0,'Bang/Hard'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Bang/Hard'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Bang/Hard'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Bang/Hard'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Bang/Hard'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Bang/Hard'] = fbhe_vector[5]  # URL
    
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table)
  else:
    fbhe_return = "No Data Found"
  
  # explain the report here
  fbhe_return4 = "Explain the Report Here"
  
  return fbhe_return, ' ', ' ', fbhe_return4

@anvil.server.callable
def srv_eff(disp_league, disp_gender, disp_year, 
                    disp_team, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout
               ):
  # return a markdown text to display
  # given the parameters

  ########### Serving Effectiveness  #####################

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
             "Zone 3":[0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,' '],
             'No Zone':[0,0,0,0,0,' '],
             ' - ':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'srv' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1, 3, 5
    column = ['Zone 1','Zone 3','Zone 5']
    for i in [1,2,3]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['serve_src_zone_net']==(i-1)*2 +1], disp_player, 'srv' )  # trying to get to 1, 3, 5
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    # now those whitoput a zone
    tmp_df = m_ppr_df[ m_ppr_df['serve_src_zone_net'] != 1 ]
    tmp_df = tmp_df[ tmp_df['serve_src_zone_net'] != 3]
    tmp_df = tmp_df[ tmp_df['serve_src_zone_net'] != 5]
    fbhe_vector = fbhe( tmp_df,
                       disp_player, 
                       'srv' )
    fbhe_table.at[0,'No Zone'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'No Zone'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'No Zone'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'No Zone'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'No Zone'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'No Zone'] = fbhe_vector[5]  # URL
    
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table)
  else:
    fbhe_return = "No Data Found"
  
  # explain the report here
  fbhe_return4 = "Explain the Report Here"
  
  return fbhe_return, ' ', ' ', fbhe_return4


@anvil.server.callable
def fbhe_srv_dest(disp_league, disp_gender, disp_year, 
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
  df_dict = {' ':['Zone A','Zone B','Zone C','Zone D', 'Zone E'],
             'Zone 1':[0,0,0,0,0],
             'Zone 2':[0,0,0,0,0],
             'Zone 3':[0,0,0,0,0],
             'Zone 4':[0,0,0,0,0],
             'Zone 5':[0,0,0,0,0]
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )
  att_table = pd.DataFrame.from_dict( df_dict)
  url_table = pd.DataFrame.from_dict( df_dict )
  
  #print(fbhe_table)
  net_list = [1,2,3,4,5]
  depth_list = ['A','B','C','D','E']
  
  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # start loop over net, 1 - 5
    for i in net_list:
      print(f"i:{i}")
      tmp1_df = m_ppr_df[ m_ppr_df['serve_dest_zone_net'] == i]
      print(f"i = {i}, tmp1 df shape:{tmp1_df.shape}")
      for j in net_list:
        tmp2_df = tmp1_df[ tmp1_df['serve_dest_zone_depth'] == depth_list[j-1]]
        print(f"i,j = {i},{j}, tmp2 df shape:{tmp2_df.shape}")
        fbhe_vector = fbhe( tmp2_df, disp_player, 'pass' )
        fbhe_table.iloc[j-1,i] = fbhe_vector[0]
        att_table.iloc[j-1,i] = fbhe_vector[1]
        url_table.iloc[j-1,i] = fbhe_vector[5]

    # now create the markdown text to return
    print(fbhe_table)
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table)
    fbhe_return2 = pd.DataFrame.to_markdown(att_table)
    fbhe_return3 = pd.DataFrame.to_markdown(url_table)
    fbhe_return4 = "Explain the data here"
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return, fbhe_return2, fbhe_return3, fbhe_return4

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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['att_src_zone_net']==i], disp_player, 'att' )
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
  
  # explain the report here
  fbhe_return4 = "Explain the Report Here"
  
  return fbhe_return, ' ', ' ', fbhe_return4

@anvil.server.callable
def error_density(disp_league, disp_gender, disp_year, 
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

  # for this one, now limit to plays that include disp plaer:
  m_ppr_df = m_ppr_df[ ( m_ppr_df['player_a1'] == disp_player ) | 
                        ( m_ppr_df['player_a2'] == disp_player ) |  
                        ( m_ppr_df['player_b1'] == disp_player ) |
                        ( m_ppr_df['player_b2'] == disp_player )
  ]

    
  print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  # create the output dataframe - This is speficif to the report
  df_dict = {'Error Density':[0],
             'Percentile':[0],
             'First Ball Errors':[0],
             'Service Errors':[0],
             'Transition Errors':[0],
             'Total Errors':[0],
             'Total Points':[0]
            }
  error_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    error_table['Total Points'] = m_ppr_df.shape[0]
    error_table['First Ball Errors'] = m_ppr_df[ ( m_ppr_df['point_outcome'] == 'FBE') & (m_ppr_df['att_player'] == disp_player ) ].shape[0]
    error_table['Transition Errors'] = m_ppr_df[ ( m_ppr_df['point_outcome'] == 'TE') & (disp_player in m_ppr_df['point_outcome_team']) ].shape[0]*0.5
    error_table['Service Errors'] = m_ppr_df[ ( m_ppr_df['point_outcome'] == 'TSE') & (m_ppr_df['serve_player'] == disp_player ) ].shape[0]
    error_table['Total Errors'] = error_table['Service Errors'] + error_table['First Ball Errors'] + error_table['Transition Errors']
    error_table['Error Density'] = error_table['Total Errors'] / error_table['Total Points']
    error_table['Percentile'] = 0
    
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(error_table)
  else:
    fbhe_return = "No Data Found"
  
  # explain the report here
  fbhe_return4 = "Explain the Report Here"
  
  return fbhe_return, ' ', ' ', fbhe_return4


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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['att_src_zone_net']==i], disp_player, 'att' )
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
  
  # explain the report here
  fbhe_return4 = "Explain the Report Here"
  
  return fbhe_return, ' ', ' ', fbhe_return4