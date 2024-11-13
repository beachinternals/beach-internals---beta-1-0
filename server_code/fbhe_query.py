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
  #print(f"FBHE_table_query: League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}, Player:{disp_player}")
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
  #print(f"FBHE_scout_query: League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}, Player:{disp_player}")
  m_ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, True )

  # now, narrow the data by competition level and dates
  m_ppr_df = ppr_df_limit( m_ppr_df, 
                          comp_l1_checked, disp_comp_l1, 
                          comp_l2_checked, disp_comp_l2, 
                          comp_l3_checked, disp_comp_l3, 
                          date_checked, disp_start_date, disp_end_date
                         )
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  # if the eata is not empty, create my df, populate it, and return it
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
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

  return fbhe_return, ' ', ' '

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
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

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
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'pass' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # Option
    tmp_ppr_df = m_ppr_df[m_ppr_df['tactic'] == "option"]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'pass' )
    fbhe_table.at[0,'Option'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Option'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Option'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Option'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Option'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Option'] = fbhe_vector[5]  # URL

    # Behind
    tmp_ppr_df = m_ppr_df[m_ppr_df['tactic'] == "behind"]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'pass' )
    fbhe_table.at[0,'Behind'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Behind'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Behind'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Behind'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Behind'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Behind'] = fbhe_vector[5]  # URL

    # Tempo
    tmp_ppr_df = m_ppr_df[(m_ppr_df['set_height'] <= 2.5) & 
                          (m_ppr_df['set_height'] != 0 ) &
                          (m_ppr_df['set_dist'] <= 2) &
                           (m_ppr_df['set_dist'] != 0 ) &
                          (m_ppr_df['tactic'] != 'option')  &
                          ( m_ppr_df['tactic'] != 'behind' ) 
    ]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'pass' )
    fbhe_table.at[0,'Tempo'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Tempo'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Tempo'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Tempo'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Tempo'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Tempo'] = fbhe_vector[5]  # URL

    # Push to Pin
    tmp_ppr_df = m_ppr_df[ ( (m_ppr_df['att_src_zone_net'] == 1) | (m_ppr_df['att_src_zone_net'] == 5) &
                            (m_ppr_df['set_dist'] > 3) & 
                            (m_ppr_df['tactic'] != 'option')  &
                            ( m_ppr_df['tactic'] != 'behind' ) )
                          ]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'pass' )
    fbhe_table.at[0,'Push to Pin'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Push to Pin'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Push to Pin'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Push to Pin'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Push to Pin'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Push to Pin'] = fbhe_vector[5]  # URL

    # Other
    tmp_ppr_df = m_ppr_df[  (m_ppr_df['tactic'] != 'behind' ) & 
                            (m_ppr_df['tactic'] != 'option' ) &
                            ~( (m_ppr_df['att_src_zone_net'] == 1) | (m_ppr_df['att_src_zone_net'] == 5) &
                              (m_ppr_df['set_dist'] > 3) ) & 
                            ~ ( (m_ppr_df['set_height'] <= 2.5) & 
                              (m_ppr_df['set_height'] != 0 ) &
                              (m_ppr_df['set_dist'] <= 2) &
                              (m_ppr_df['set_dist'] != 0 ) )
                          ]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'pass' )
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
  
  return fbhe_return, ' ', ' '

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
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  # create the output dataframe - This is speficif to the report
  df_dict = {' ':['FBHE','Kills','Errors','Attempts', ' ','URL'],
             'All':[0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 3','Zone 5']
    for i in [0,1,2]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['serve_src_zone_net']==(i*2)+1], disp_player, 'pass' )
      fbhe_table.at[0,column[i]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i]] = fbhe_vector[5]  # URL

    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table)
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return, ' ', ' '

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
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

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
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
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
    fbhe_vector = fbhe_attack_type( m_ppr_df, disp_player, 'poke')
    fbhe_table.at[0,'Poke/Roll'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Poke/Roll'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Poke/Roll'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Poke/Roll'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Poke/Roll'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Poke/Roll'] = fbhe_vector[5]  # URL

    # calculate for shot, imit the data, and call fbhe
    fbhe_vector = fbhe_attack_type( m_ppr_df, disp_player, 'shoot')
    fbhe_table.at[0,'Shot'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Shot'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Shot'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Shot'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Shot'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Shot'] = fbhe_vector[5]  # URL

        # calculate for poke/roll
    # limit the data, and call fbhe
    fbhe_vector = fbhe_attack_type( m_ppr_df, disp_player, 'bang')
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
  
  return fbhe_return, ' ', ' '

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
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  # create the output dataframe - This is speficif to the report
  df_dict = {' ':['FBHE','Kills','Errors','Attempts', ' ','URL','Aces','Errors'],
             'All':[0,0,0,0,0,' ',0,0],
             'Zone 1':[0,0,0,0,0,' ',0,0],
             "Zone 3":[0,0,0,0,0,' ',0,0],
             'Zone 5':[0,0,0,0,0,' ',0,0],
             'No Zone':[0,0,0,0,0,' ',0,0]
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'srv' )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL
    # Aces and Errors
    tmp_df = m_ppr_df[m_ppr_df['point_outcome'] == "TSA"]
    print(f"Aces {tmp_df.shape[0]}")
    tmp_df = tmp_df[tmp_df['serve_player'].str.strip() == disp_player.strip() ]
    fbhe_table.at[6,'All'] = tmp_df.shape[0]
    tmp_df = m_ppr_df[ m_ppr_df['point_outcome'] == "TSE" ]
    print(f"Errors {tmp_df.shape[0]}")
    tmp_df = tmp_df[tmp_df['serve_player'].str.strip() == disp_player.strip() ]    
    fbhe_table.at[7,'All'] = tmp_df.shape[0]

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
      tmp_df = m_ppr_df[ m_ppr_df['point_outcome'] == "TSA" ]
      tmp_df = tmp_df[tmp_df['serve_player'].str.strip() == disp_player.strip()] 
      tmp_df = tmp_df[tmp_df['serve_src_zone_net']==(i-1)*2+1 ]
      fbhe_table.at[6,column[i-1]] = tmp_df.shape[0]
      tmp_df = m_ppr_df[ m_ppr_df['point_outcome'] == "TSE" ]
      tmp_df = tmp_df[tmp_df['serve_player'].str.strip() == disp_player.strip()] 
      tmp_df = tmp_df[tmp_df['serve_src_zone_net']==(i-1)*2+1 ]
      fbhe_table.at[7,column[i-1]] = tmp_df.shape[0]

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
  
  return fbhe_return, ' ', ' '


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
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

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
      #print(f"i:{i}")
      tmp1_df = m_ppr_df[ m_ppr_df['pass_src_zone_net'] == i]
      #print(f"i = {i}, tmp1 df shape:{tmp1_df.shape}")
      for j in net_list:
        tmp2_df = tmp1_df[ tmp1_df['pass_src_zone_depth'] == depth_list[j-1]]
        #print(f"i,j = {i},{j}, tmp2 df shape:{tmp2_df.shape}")
        fbhe_vector = fbhe( tmp2_df, disp_player, 'pass' )
        #print(fbhe_vector)
        fbhe_table.iloc[j-1,i] = fbhe_vector[0]
        att_table.iloc[j-1,i] = fbhe_vector[3]
        url_table.iloc[j-1,i] = fbhe_vector[5]

    # now create the markdown text to return
    #print(fbhe_table)
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table)
    fbhe_return2 = pd.DataFrame.to_markdown(att_table)
    fbhe_return3 = pd.DataFrame.to_markdown(url_table)
    fbhe_return4 = "Explain the data here"
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return, fbhe_return2, fbhe_return3

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
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

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
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
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
  
  return fbhe_return, ' ', ' '

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

  # for this one, now limit to plays that include disp player:
  m_ppr_df = m_ppr_df[ ( m_ppr_df['player_a1'].str.strip() == disp_player.strip() ) | 
                        ( m_ppr_df['player_a2'].str.strip() == disp_player.strip() ) |  
                        ( m_ppr_df['player_b1'].str.strip() == disp_player.strip() ) |
                        ( m_ppr_df['player_b2'].str.strip() == disp_player.strip() )
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
    error_vector = calc_error_den(m_ppr_df, disp_player)
    error_table['Error Density'] = error_vector[0]
    error_table['Percentile'] = error_vector[1]
    error_table['First Ball Errors'] = error_vector[2]
    error_table['Service Errors'] = error_vector[3]
    error_table['Transition Errors'] = error_vector[4]
    error_table['Total Errors'] = error_vector[5]      
    error_table['Total Points'] = error_vector[6]
    
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(error_table)
  else:
    fbhe_return = "No Data Found"

  return fbhe_return, ' ', ' '

#######################  Transition  ############################
@anvil.server.callable
def tcr(disp_league, disp_gender, disp_year, 
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
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  # create the output dataframe - This is speficif to the report
  df_dict = {' ':['% Points Won','Percentile','% of Transition','Kills Earned', 'Errors Recieved ','Kills Lost','Errors Given','Points Earned','Points Lost','Total Points'],
             'All':[0,0,0,0,0,0,0,0,0,0],
             'Serving':[0,0,0,0,0,0,0,0,0,0],
             'Receiving':[0,0,0,0,0,0,0,0,0,0]
            }
  trans_table = pd.DataFrame.from_dict( df_dict, orient='columns' )
  #print(trans_table)
  trans_list = [0,0,0,0,0,0,0,0,0,0]
  # now delete column 0 
  #trans_table = trans_table.drop(columns=[0])
  #print(trans_table)

  
  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")

    # calculate data for all attempts
    trans_list = calc_trans( m_ppr_df, disp_player, 'all')
    #print(f"Transition List, All:{trans_list}")
    trans_table.at[0,'All'] = trans_list[0]
    trans_table.at[1,'All'] = trans_list[1]
    trans_table.at[2,'All'] = trans_list[2]
    trans_table.at[3,'All'] = trans_list[3]
    trans_table.at[4,'All'] = trans_list[4]
    trans_table.at[5,'All'] = trans_list[5]
    trans_table.at[6,'All'] = trans_list[6]
    trans_table.at[7,'All'] = trans_list[7]
    trans_table.at[8,'All'] = trans_list[8]
    trans_table.at[9,'All'] = trans_list[9]

    # calculate data for all attempts
    trans_list = calc_trans( m_ppr_df, disp_player, 'srv')
    #print(f"Transition List, Srv:{trans_list}")
    trans_table.at[0,'Serving'] = trans_list[0]
    trans_table.at[1,'Serving'] = trans_list[1]
    trans_table.at[2,'Serving'] = trans_list[2]
    trans_table.at[3,'Serving'] = trans_list[3]
    trans_table.at[4,'Serving'] = trans_list[4]
    trans_table.at[5,'Serving'] = trans_list[5]
    trans_table.at[6,'Serving'] = trans_list[6]
    trans_table.at[7,'Serving'] = trans_list[7]
    trans_table.at[8,'Serving'] = trans_list[8]
    trans_table.at[9,'Serving'] = trans_list[9]

    # calculate data for Reeive attempts
    trans_list = calc_trans( m_ppr_df, disp_player, 'rcv')
    #print(f"Transition List, rcv:{trans_list}")
    trans_table.at[0,'Receiving'] = trans_list[0]
    trans_table.at[1,'Receiving'] = trans_list[1]
    trans_table.at[2,'Receiving'] = trans_list[2]
    trans_table.at[3,'Receiving'] = trans_list[3]
    trans_table.at[4,'Receiving'] = trans_list[4]
    trans_table.at[5,'Receiving'] = trans_list[5]
    trans_table.at[6,'Receiving'] = trans_list[6]
    trans_table.at[7,'Receiving'] = trans_list[7]
    trans_table.at[8,'Receiving'] = trans_list[8]
    trans_table.at[9,'Receiving'] = trans_list[9]


    # now create the markdown text to return
    trans_return = pd.DataFrame.to_markdown(trans_table)
  else:
    trans_return = "No Data Found"

  return trans_return, ' ', ' '

@anvil.server.callable
def expected_value(disp_league, disp_gender, disp_year, 
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
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  # create the output dataframe - This is speficif to the report
  df_dict = {' ':['Expected Value',
                  'Points Won','First Ball Kill','Transition Kill','Opponent Transition Error','Terminal Serve Error',
                  'Points Lost','First Ball Error','Transition Error','Opponent Transition Kill','Terminal Serve Ace'
                  ],
             'All':[0,0,0,0,0,0,0,0,0,0,0]
            }
  df_dict = {}
  ev_table = pd.DataFrame.from_dict( df_dict )

  # now filter my ppr file to just those wher ethe disp_player receives serve
  ppr_df = m_ppr_df[ m_ppr_df['pass_player'].str.strip() == disp_player.strip()]
  
  ############### Third Populate the dataframe, assuming we have data returned
  if ppr_df.shape[0] > 0:
    ev_vector = calc_ev(ppr_df, disp_player
                       )
    ev_table.loc['Expected Value','All'] = ev_vector[0]
    ev_table.loc['Total Points','All'] = ev_vector[1]
    ev_table.loc['  ','All'] = ' '
    ev_table.loc['Points Won','All'] = ev_vector[2]
    ev_table.loc['---------------------','All'] = ' '
    ev_table.loc['First Ball Kill','All'] = ev_vector[3] 
    ev_table.loc['Transition Kill Earned','All'] =ev_vector[4]
    ev_table.loc['Transition Error Received','All']=ev_vector[5]
    ev_table.loc['Service Errors Earned','All']=ev_vector[6]

    ev_table.loc['.  ','All'] = ' '
    ev_table.loc['Points Lost','All'] = ev_vector[7]
    ev_table.loc['----------------------','All'] = ' '
    ev_table.loc['First Ball Error','All']=ev_vector[8]
    ev_table.loc['Transition Error Given','All']=ev_vector[9]
    ev_table.loc['Transition Kill Lost','All']=ev_vector[10]
    ev_table.loc['Service Ace Lost','All']=ev_vector[11]
 
    # now create the markdown text to return
    ev_return = pd.DataFrame.to_markdown(ev_table)
  else:
    ev_return = "No Data Found"
  
  return ev_return, ' ', ' '

@anvil.server.callable
def report_stuba(disp_league, disp_gender, disp_year, 
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
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

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
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
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

  
  return fbhe_return, ' ', ' '