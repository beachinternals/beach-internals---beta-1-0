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
from anvil import pdf
from pair_functions import *
from matchup_reports import player_45_serves
from plot_functions import *

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

@anvil.server.callable
def player_attacking(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    ):

  # put togehter three attacking tables into one report:
  # fbhe_scout_query
  # fbhe by fbhe_by attack tactic
  table1_mkdn = ''
  table2_mkdn = ''
  table3_mkdn = ''
  a = ''
  b = ''
  
  table1_mkdn,a,b = fbhe_scout_query(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    )
  
  table2_mkdn,a,b = player_attack_as_passer(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
               )

  table3_mkdn,a,b = fbhe_by_srv_src(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
               )

  return table1_mkdn, table2_mkdn, table3_mkdn
  
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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['att_src_zone_net']==i], disp_player, 'att', True )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return

@anvil.server.callable
def fbhe_scout_query(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    ):

  
  # return a fbhe of a given player
  # we will return the text as markdown to be displayed in a rich text box in the client
  # the difference here is that we combine the scout file with the team file to query aginst.
  # double checked that they are logged in
  # scout should always be true here

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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att', True )
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
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['att_src_zone_net']==zone], disp_player, 'att', True )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[2,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[3,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[4,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[1,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL

    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"

  return fbhe_return, ' ', ' '

@anvil.server.callable
def player_attack_as_passer(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
                    ):

  
  # return a fbhe of a given player
  # we will return the text as markdown to be displayed in a rich text box in the client
  # the difference here is that we combine the scout file with the team file to query aginst.
  # double checked that they are logged in
  # scout should always be true here

  # create the output dataframe
  df_dict = {' ':['FBHE','FBSO','Kills','Errors','Attempts','% Out of System','URL'],
             'All':[0,0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,0,' '],
             "Zone 2":[0,0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,0,' '],
             'Zone 4':[0,0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,0,' '],
             'No Zone':[0,0,0,0,0,0,' '],
             'Option':[0,0,0,0,0,0,' ']
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

  ppr_df_option = m_ppr_df[ m_ppr_df['tactic'] == 'option']
  ppr_df_no_option = m_ppr_df[ m_ppr_df['tactic'] != 'option']
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  # if the eata is not empty, create my df, populate it, and return it
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'pass', True )
    oos_vector1 = count_out_of_system(m_ppr_df,disp_player,'pass')
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[2,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[3,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[4,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[1,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = str("{:.0%}").format(oos_vector1[1])  # percent out of system
    fbhe_table.at[6,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5','No Zone']
    for i in [1,2,3,4,5,6]:
      zone = 0 if i == 6 else i
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['att_src_zone_net']==zone], disp_player, 'pass', True )
      oos_vector1 = count_out_of_system(ppr_df_no_option[ppr_df_no_option['att_src_zone_net']==i], disp_player, 'pass')
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[2,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[3,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[4,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[1,column[i-1]] = fbhe_vector[4]  # FBSO
      fbhe_table.at[5,column[i-1]] = str("{:.0%}").format(oos_vector1[1])  # Out of System      
      fbhe_table.at[6,column[i-1]] = fbhe_vector[5]  # URL

    # calculate fbhe for all option
    fbhe_vector = fbhe( ppr_df_option, disp_player, 'pass', True )
    oos_vector1 = count_out_of_system(ppr_df_option,disp_player,'pass')
    fbhe_table.at[0,'Option'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[2,'Option'] = fbhe_vector[1]  # attacks
    fbhe_table.at[3,'Option'] = fbhe_vector[2]  # errors
    fbhe_table.at[4,'Option'] = fbhe_vector[3]  # attempts
    fbhe_table.at[1,'Option'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Option'] = str("{:.0%}").format(oos_vector1[1])  # percent out of system
    fbhe_table.at[6,'Option'] = fbhe_vector[5]  # URL

    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"

  return fbhe_return, ' ', ' '

@anvil.server.callable
def fbhe_by_attack_tactic(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'pass', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # Option
    tmp_ppr_df = m_ppr_df[m_ppr_df['tactic'] == "option"]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'pass', True )
    fbhe_table.at[0,'Option'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Option'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Option'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Option'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Option'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Option'] = fbhe_vector[5]  # URL

    # Behind
    tmp_ppr_df = m_ppr_df[m_ppr_df['tactic'] == "behind"]
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'pass', True )
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
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'pass', True )
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
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'pass', True )
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
    fbhe_vector = fbhe( tmp_ppr_df, disp_player, 'pass', True )
    fbhe_table.at[0,'Other'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Other'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Other'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Other'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Other'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Other'] = fbhe_vector[5]  # URL
    
    # Now, convert to a markdown file
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return, ' ', ' '

@anvil.server.callable
def fbhe_by_srv_src(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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
  df_dict = {' ':['FBHE','FBSO','Kills','Errors','Attempts','URL'],
             'All':[0,0,0,0,0,' '],
             'Zone 1':[0,0,0,0,0,' '],
             'Zone 3':[0,0,0,0,0,' '],
             'Zone 5':[0,0,0,0,0,' '],
             'No Zone':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'pass', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[2,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[3,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[4,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[1,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 3','Zone 5','No Zone']
    for i in [0,1,2,3]:
      zone = 0 if i == 3 else (i*2)+1
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['serve_src_zone_net']==zone], disp_player, 'pass', True )
      fbhe_table.at[0,column[i]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[2,column[i]] = fbhe_vector[1]  # attacks
      fbhe_table.at[3,column[i]] = fbhe_vector[2]  # errors
      fbhe_table.at[4,column[i]] = fbhe_vector[3]  # attempts
      fbhe_table.at[1,column[i]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i]] = fbhe_vector[5]  # URL

    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return, ' ', ' '

@anvil.server.callable
def fbhe_by_attack_type(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att', True )
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
    fbhe_vector = fbhe_attack_type( m_ppr_df, disp_player, 'poke', True)
    fbhe_table.at[0,'Poke/Roll'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Poke/Roll'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Poke/Roll'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Poke/Roll'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Poke/Roll'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Poke/Roll'] = fbhe_vector[5]  # URL

    # calculate for shot, imit the data, and call fbhe
    fbhe_vector = fbhe_attack_type( m_ppr_df, disp_player, 'shoot', True)
    fbhe_table.at[0,'Shot'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Shot'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Shot'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Shot'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Shot'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Shot'] = fbhe_vector[5]  # URL

        # calculate for poke/roll
    # limit the data, and call fbhe
    fbhe_vector = fbhe_attack_type( m_ppr_df, disp_player, 'bang', True)
    fbhe_table.at[0,'Bang/Hard'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Bang/Hard'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Bang/Hard'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Bang/Hard'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Bang/Hard'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Bang/Hard'] = fbhe_vector[5]  # URL
    
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return, ' ', ' '

@anvil.server.callable
def srv_eff(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
               ):
  # if the disp_pair is set to ayting (either not null or len > 0), then limit to the pair
  # return a markdown text to display
  # given the parameters
  #
  ########### Serving Effectiveness  #####################

  ############## First - Get the Data, and limit it by the parameters - Generaic for all reports
  m_ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, scout )
  m_ppr_df = ppr_df_limit( m_ppr_df, 
                          comp_l1_checked, disp_comp_l1, 
                          comp_l2_checked, disp_comp_l2, 
                          comp_l3_checked, disp_comp_l3, 
                          date_checked, disp_start_date, disp_end_date
                         )

  # now, limit to the pair, if needed
  if len(disp_pair.strip()) > 0:
    m_ppr_df = pair_filter( m_ppr_df, disp_pair )
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  # create the output dataframe - This is speficif to the report
  df_dict = {' ':['FBHE','FBSO','Opp Kills','Opp Errors','Attempts', 'URL','Service Aces','Service Errors'],
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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'srv', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[2,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[3,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[4,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[1,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL
    # Aces and Errors
    tmp_df = m_ppr_df[m_ppr_df['point_outcome'] == "TSA"]
    #print(f"Aces {tmp_df.shape[0]}")
    tmp_df = tmp_df[tmp_df['serve_player'].str.strip() == disp_player.strip() ]
    fbhe_table.at[6,'All'] = tmp_df.shape[0]
    tmp_df = m_ppr_df[ m_ppr_df['point_outcome'] == "TSE" ]
    #print(f"Errors {tmp_df.shape[0]}")
    tmp_df = tmp_df[tmp_df['serve_player'].str.strip() == disp_player.strip() ]    
    fbhe_table.at[7,'All'] = tmp_df.shape[0]

    # calculate for zones 1, 3, 5
    column = ['Zone 1','Zone 3','Zone 5']
    for i in [1,2,3]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['serve_src_zone_net']==(i-1)*2 +1], disp_player, 'srv', True )  # trying to get to 1, 3, 5
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[2,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[3,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[4,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[1,column[i-1]] = fbhe_vector[4]  # confidence interval
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
                       'srv', True )
    fbhe_table.at[0,'No Zone'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[2,'No Zone'] = fbhe_vector[1]  # attacks
    fbhe_table.at[3,'No Zone'] = fbhe_vector[2]  # errors
    fbhe_table.at[4,'No Zone'] = fbhe_vector[3]  # attempts
    fbhe_table.at[1,'No Zone'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'No Zone'] = fbhe_vector[5]  # URL
    
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return, ' ', ' '


@anvil.server.callable
def fbhe_srv_dest(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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
        fbhe_vector = fbhe( tmp2_df, disp_player, 'pass', True )
        #print(fbhe_vector)
        fbhe_table.iloc[j-1,i] = fbhe_vector[0]
        att_table.iloc[j-1,i] = fbhe_vector[3]
        url_table.iloc[j-1,i] = fbhe_vector[5]

    # now create the markdown text to return
    #print(fbhe_table)
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
    fbhe_return2 = pd.DataFrame.to_markdown(att_table, index = False )
    fbhe_return3 = pd.DataFrame.to_markdown(url_table, index = False )
  else:
    fbhe_return = "No Data Found"
    fbhe_return2 = ''
    fbhe_return3 = ''
  
  return fbhe_return, fbhe_return2, fbhe_return3

@anvil.server.callable
def tri_score(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
               ):
  
  # return a markdown text to display
  # given the parameters

  ############## First - Get the Data, and limit it by the parameters - Generaic for all reports
  tri_df, tri_data_found = get_tri_data( disp_league, disp_gender, disp_year, date_checked, disp_start_date, disp_end_date )
  #print(f" Data Found? {tri_data_found}m=, records={tri_df.shape[0]}, Displayer Player: {disp_player}")
  
  if (tri_df.shape[0] == 0):
    return "No Triangle Scoring Data Found"

  disp_player = disp_player.strip()
  tri1_df = tri_df[ tri_df['player_a1'].str.strip() == disp_player ]
  tri2_df = tri_df[ tri_df['player_a2'].str.strip() == disp_player ]
  tri3_df = tri_df[ tri_df['player_b1'].str.strip() == disp_player ]
  tri4_df = tri_df[ tri_df['player_b2'].str.strip() == disp_player ]

  tri_df = pd.concat([ tri1_df, tri2_df, tri3_df, tri4_df ])
  #print(f"tri_df size:{tri_df.shape[0]}, Tri1 = {tri1_df.shape[0]}, Tri2 = {tri2_df.shape[0]}, Tri3 = {tri3_df.shape[0]}, Tri4 = {tri4_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  # create the output dataframe - This is speficif to the report
  tri_dict = {'Team A':[' '],
             'Team B':[' '],
              'Winner':[' '],
              'Set':[0],
             'TS +/-':[0],
             'FB +/-':[0],
             'Tran +/-':[0],
             'FBHE':[0],
              'FBHE Diff':[0],
             'TCR':[0],
             'Err Den':[0]
            }
  tri_table = pd.DataFrame.from_dict( tri_dict )


  ############### Third Populate the dataframe, assuming we have data returned
  num_row = tri_df.shape[0]
  #print(f"Number of Rows in Tri Data:{num_row}")
  #print(tri_df)
  i = 0
  for index, row in tri_df.iterrows():
    #print(f"i: {i}, Tri Table:{tri_table}")

    if ( i != 0):
      # need to add a row to tri_table
      tri_table.loc[len(tri_table.index)] = tri_dict
      
    disp_team = row['teama'] if disp_player.strip() in row['teama'].strip() else row['teamb']
    tri_table.at[i,'Team A'] = row['teama']
    tri_table.at[i,'Team B'] = row['teamb']
    tri_table.at[i,'Winner'] = row['winning_team']
    tri_table.at[i,'Set'] = row['set']
    tri_table.at[i,'TS +/-'] = row['tsrv_adv_a'] if disp_team.strip() in row['teama'].strip() else -row['tsrv_adv_a']
    tri_table.at[i,'FB +/-'] = row['fb_adv_a']if disp_team.strip() in row['teama'].strip()  else -row['fb_adv_a']
    tri_table.at[i,'Tran +/-'] = '{:.3}'.format(row['tran_adv_a']) if disp_team.strip() in row['teama'].strip()  else '{:.3}'.format(-row['tran_adv_a'])
    tri_table.at[i,'FBHE'] = '{:.3}'.format(row['fbhe_a_noace']) if disp_team.strip() in row['teama'].strip()  else '{:.3}'.format(-row['fbhe_b_noace'])
    tri_table.at[i,'FBHE Diff'] = '{:.3}'.format(row['fbhe_diff_noace']) if disp_team.strip() in row['winning_team'].strip() else '{:.3}'.format(-row['fbhe_diff_noace'])
    tri_table.at[i,'TCR'] = '{:.1%}'.format(row['tcr_a']) if disp_team.strip() in row['teama'].strip()  else '{:.1%}'.format(row['tcr_b'])
    tri_table.at[i,'Err Den'] = '{:.1%}'.format(row['err_den_a']) if disp_team.strip() in row['teama'].strip()  else '{:.1%}'.format(row['err_den_b'])
    i = i + 1
    
  # now create the markdown text to return
  tri_return = pd.DataFrame.to_markdown(tri_table, index = False )
  
  return tri_return, ' ', ' '

@anvil.server.callable
def error_density(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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

  # check if no data left:
  if m_ppr_df.shape[0] > 0:
    # for this one, now limit to plays that include disp player:
    m_ppr_df = m_ppr_df[ ( m_ppr_df['player_a1'].str.strip() == disp_player.strip() ) | 
                          ( m_ppr_df['player_a2'].str.strip() == disp_player.strip() ) |  
                          ( m_ppr_df['player_b1'].str.strip() == disp_player.strip() ) |
                          ( m_ppr_df['player_b2'].str.strip() == disp_player.strip() )
    ]
    #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

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
    error_vector = calc_error_den(m_ppr_df, disp_player)
    error_table['Error Density'] = error_vector[0]
    error_table['Percentile'] = error_vector[1]
    error_table['First Ball Errors'] = error_vector[2]
    error_table['Service Errors'] = error_vector[3]
    error_table['Transition Errors'] = error_vector[4]
    error_table['Total Errors'] = error_vector[5]      
    error_table['Total Points'] = error_vector[6]
    
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(error_table, index = False )
  else:
    fbhe_return = "No Data Found"

  return fbhe_return, ' ', ' '

#######################  Transition  ############################
@anvil.server.callable
def tcr(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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
    trans_return = pd.DataFrame.to_markdown(trans_table, index = False )
  else:
    trans_return = "No Data Found"

  return trans_return, ' ', ' '

@anvil.server.callable
def expected_value(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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

  # now filter my ppr file to just those where the disp_player receives serve
  if m_ppr_df.shape[0] > 0:
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
      ev_return = pd.DataFrame.to_markdown(ev_table )
    else:
      ev_return = "No Data Found"
  else:
     ev_return = "No Data Found"
  
  return ev_return, ' ', ' '

@anvil.server.callable
def out_of_system(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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

  # now limit to only out of system plays
  m_ppr_df = m_ppr_df[ m_ppr_df['pass_oos'] != 0 ]
  #m_ppr_df = m_ppr_df[ m_ppr_df['pass_player'] == disp_player ]
  
  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'pass', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['pass_src_zone_net']==i], disp_player, 'pass', True )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL
 
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return, ' ', ' '

@anvil.server.callable
def out_of_system_angle(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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

  # now limit to only out of system plays
  m_ppr_df = m_ppr_df[ m_ppr_df['pass_oos'] == 1 ]
  
  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'pass', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['pass_src_zone_net']==i], disp_player, 'pass', True )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL
 
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return, ' ', ' '

@anvil.server.callable
def out_of_system_location(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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

  # now limit to only out of system plays
  m_ppr_df = m_ppr_df[ m_ppr_df['pass_oos'] == 10 ]
  
  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'pass', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['pass_src_zone_net']==i], disp_player, 'pass', True )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL
 
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return, ' ', ' '

@anvil.server.callable
def out_of_system_height(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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

  # now limit to only out of system plays
  m_ppr_df = m_ppr_df[ m_ppr_df['pass_oos'] == 100 ]
  
  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'pass', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['pass_src_zone_net']==i], disp_player, 'pass', True )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL
 
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"
  
  return fbhe_return, ' ', ' '


  
@anvil.server.callable
def fbhe_in_out_system(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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
             'In System':[0,0,0,0,0,' '],
             "Out of System":[0,0,0,0,0,' '],
             'Option - In System':[0,0,0,0,0,' '],
             'Option - Out of System':[0,0,0,0,0,' ']
            }
  fbhe_table = pd.DataFrame.from_dict( df_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  if m_ppr_df.shape[0] > 0:
    # calculate fbhe for all attacks
    #print(f"Calling fbhe:{m_ppr_df.shape}, {disp_player}")
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'pass', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # in system only
    fbhe_vector = fbhe( m_ppr_df[ m_ppr_df['pass_oos'] == 0], disp_player, 'pass', True )
    fbhe_table.at[0,'In System'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'In System'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'In System'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'In System'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'In System'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'In System'] = fbhe_vector[5]  # URL

    # OUT OF  system only
    fbhe_vector = fbhe( m_ppr_df[ m_ppr_df['pass_oos'] != 0 ], disp_player, 'pass', True )
    fbhe_table.at[0,'Out of System'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Out of System'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Out of System'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Out of System'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Out of System'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Out of System'] = fbhe_vector[5]  # URL

    # option, in system
    fbhe_vector = fbhe( m_ppr_df[ (m_ppr_df['tactic'] == 'option') & (m_ppr_df['pass_oos'] == 0)], disp_player, 'pass', True )
    fbhe_table.at[0,'Option - In System'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Option - In System'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Option - In System'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Option - In System'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Option - In System'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Option - In System'] = fbhe_vector[5]  # URL

    # option, out of system
    fbhe_vector = fbhe( m_ppr_df[ (m_ppr_df['tactic'] == 'option') & (m_ppr_df['pass_oos'] != 0)], disp_player, 'pass', True )
    fbhe_table.at[0,'Option - Out of System'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'Option - Out of System'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'Option - Out of System'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'Option - Out of System'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'Option - Out of System'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'Option - Out of System'] = fbhe_vector[5]  # URL

 
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"

  
  return fbhe_return, ' ', ' '


@anvil.server.callable
def player_sum_rpt(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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

  # buid three dataframes to display:
  # scor_df : for hte scoring column
  # rot_df : for teh rotations COlumn
  # stat_df : for the stats column

  
  scor_dict = {' ':['Pt Diff','Term Srv','Live Rallies', '',
                    'SideOut', 'Opp Sideout','',
                    'FB Stop', 'Opp FB Stop','',
                    'FB Win','',
                    'Trans Win','Opp Trans Win','',
                    'Blocking','Digging','Create','',
                   'Serving','First Ball Attacking','Transition Attacking'],
             '#':[0,0,0,' ', 0,0,' ',0,' ',0,0,' ',0,0,0,' ',0,0,0],
             '%':[0,0,0,' ', 0,0,' ',0,' ',0,0,' ',0,0,0,' ',0,0,0]
            }
  scor_table = pd.DataFrame.from_dict( scor_dict )

  ############### Third Populate the dataframe, assuming we have data returned
  # Call number of points
  pts_df = point_totals(m_ppr_df,disp_player)
  if pts_df[0,'pts_total'] != 0:
    # now store the values into t scor_return array
    # point differential
    scor_table[0,'#'] = ( ( pts_df[0,'p_tsa'] + pts_df[0,'p_fbk'] + pts_df[0,'p_tk'] + pts_df[0,'o_tse'] + pts_df[0,'o_fbe'] + pts_df[0,'o_te'] ) -
                          ( pts_df[0,'p_tse'] + pts_df[0,'p_fbe'] + pts_df[0,'p_te'] + pts_df[0,'o_tsa'] + pts_df[0,'o_fbk'] + pts_df[0,'o_tk'] ) )
    scor_table[0,'%'] = scor_table[0,'#']/pts_df[0,'pts_total']

    # Terminal Serves
    scor_table[1,'#'] = ( point_totals[0,'p_tsa'] + point_totals[0,'o_tse']) - ( point_totals[0,'o_tsa'] + point_totals[0,'p_tse'])
    scor_table[1,'%'] = scor_table[1,'#']/pts_df[0,'pts_total']

    # live Rallies -- guessing all transition points??
    scor_table[2,'#'] = pts_df[0,'p_tk'] + pts_df[0,'p_te'] + pts_df[0,'o_tk'] + ptd_df[0,'o_te']
    scor_table[2,"%"] = scor_table[2,'#'] / pts_df[0,'pts_total']

    # blank row
    # Side out = FBK + TK
    #----------------------------
    #... i think we have an issue here as we want TK only when we were served?  Or both TE and TK when we were served?
    #----------------------
    scor_table[4,'#'] = pts_df[0,'p_fbk'] + pts_df[0,'p_tk']
    scor_table[4,"%"] = scor_table[4,'#'] / pts_df[0,'pts_total']
    scor_table[5,'#'] = pts_df[0,'o_fbk'] + pts_df[0,'o_tk']
    scor_table[5,"%"] = scor_table[5,'#'] / pts_df[0,'pts_total']

    # first ball sideout - FBK/points
    scor_table[7,"#"] = pts_df[0,'p_fbk']
    scor_table[7,"%"] = pts_df[0,'p_fbk']/pts_df[0,'o_serves']
    scor_table[8,"#"] = pts_df[0,'o_fbk']
    scor_table[8,"%"] = pts_df[0,'o_fbk']/pts_df[0,'p_serves']

    # blank row
    # first ball stop - fbe / serves (attempts)
    scor_table[10,"#"] = pts_df[0,'o_fbe']
    scor_table[10,"%"] = pts_df[0,'o_fbe']/(pts_df[0,'p_serves'] - pts_df[0,'p_tse'])
    scor_table[11,"#"] = pts_df[0,'p_fbe']
    scor_table[11,"%"] = pts_df[0,'p_fbe']/(pts_df[0,'o_serves'] - pts_df[0,'o_tse'])

    # blank row
    # first ball win %
    scor_table[13,'%'] = ( pts_df[0,'p_fbk'] + pts_df[0,'o_fbe']) / (pts_df[0,'pts_total']-pts_df[0,'p_tse']-ptd_df[0,'o_tse'])

    # blank row
    #Transition Win - Number of transitiono points we won! and they won
 
    # now create the markdown text to return
    scor_return = pd.DataFrame.to_markdown(scor_table, index = False )
  else:
    scor_return = "No Data Found"
    
  return scor_return, ' ', ' '


@anvil.server.callable
def player_sw(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
               ):
  
  # return a markdown text to display
  # given the parameters

  ############## First - Get the Data, and limit it by the parameters - Generaic for all reports
  # for this report, we look in to the master_player file, at the s_w field.

  # unpack player into team, number and short name
  str_loc = disp_player.index(' ')
  p_team = disp_player[:str_loc].strip()
  p_player = disp_player[str_loc+1:]
  str_loc = p_player.index(' ')
  p_num = p_player[:str_loc].strip()
  p_sname = p_player[str_loc+1:].strip()
  
  sw_list = [(f_row['s_w']) for f_row in app_tables.master_player.search(league=disp_league,gender=disp_gender,year=disp_year,team=p_team,number=p_num,shortname=p_sname)]

  if sw_list[0]:
    # sw_list is now a media object, need to turn it back into a dataframe
    sw_df =  pd.read_csv(io.BytesIO( sw_list[0].get_bytes()))

    # now we are going split s & w into 3 
    # now calculate the Offense strength and weakness markdown
    off_df = sw_df[ sw_df['Section'] == 'Offense'] 
    off_df = off_df.sort_values(by='Category', ascending=True, na_position='last')
    #print(f"pair_sw_report: off_df: {off_df}")
    off_df = off_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
    off_df = off_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})

    # now calculate the Deffense strength and weakness markdown
    def_df = sw_df[ sw_df['Section'] == 'Serving'] 
    def_df = def_df.sort_values(by='Category', ascending=True, na_position='last')
    def_df = def_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
    def_df = def_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})

    # now calculate the Errors strength and weakness markdown
    err_df = sw_df[ sw_df['Section'] == 'Error & Transitison'] 
    err_df = err_df.sort_values(by='Category', ascending=True, na_position='last')
    err_df = err_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
    err_df = err_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})

    # now calculate the Errors strength and weakness markdown
    cons_df = sw_df[ sw_df['Section'] == 'Consistency'] 
    cons_df = cons_df.sort_values(by='Category', ascending=True, na_position='last')
    cons_df = cons_df[['Description','Category','Var Desc','Var Value','Var Percentile']]
    cons_df = cons_df.rename(columns={'Var Desc':'Variable','Var Value':'Value','Var Percentile':'Percentile'})
    
    # now make the markdowns
    off_mkdn = pd.DataFrame.to_markdown(off_df, index = False)
    def_mkdn = pd.DataFrame.to_markdown(def_df, index = False)
    err_mkdn = pd.DataFrame.to_markdown(err_df, index = False)
    cons_mkdn = pd.DataFrame.to_markdown(cons_df, index = False)
    
  else:
    off_mkdn = 'No Data Found'
    def_mkdn = 'No Data Found'
    err_mkdn = 'No Data Found'
    cons_mkdn = 'No Data Found'
  
  return off_mkdn, def_mkdn, err_mkdn, cons_mkdn



@anvil.server.callable
def player_consistency(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
               ):
  # return a markdown text to display
  # given the parameters

  '''
  This reports display two mkdn tables each that show he folllowing:
  - FBHE
  -TCR
  -ED
  -Knockout (serving)
  - Passing
  - Point Differential

  
  '''

  ############## First - Get the Data, and limit it by the parameters - Generaic for all reports
  m_ppr_df = get_ppr_data( disp_league, disp_gender, disp_year, disp_team, scout )
  m_ppr_df = ppr_df_limit( m_ppr_df, 
                          comp_l1_checked, disp_comp_l1, 
                          comp_l2_checked, disp_comp_l2, 
                          comp_l3_checked, disp_comp_l3, 
                          date_checked, disp_start_date, disp_end_date
                         )
  # limit the ppr data to plays with this player
  m_ppr_df = m_ppr_df[ (m_ppr_df['player_a1'] == disp_player) | (m_ppr_df['player_a2'] == disp_player) | (m_ppr_df['player_b1'] == disp_player) | (m_ppr_df['player_b2'] == disp_player) ]
    
  #print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  ############## Secomd - Create the dataframe that will be displayed as a table, report specific
  cons_table, no_data = calc_consistency_match_table( m_ppr_df, disp_player )
  cons2_table, no_data1 = calc_consistency_s2s_table( m_ppr_df, disp_player )
    
  cons_return = pd.DataFrame.to_markdown(cons_table, index = False )
  cons2_return = pd.DataFrame.to_markdown(cons2_table, index = False )

  return cons_return, cons2_return, ' ', ' '


@anvil.server.callable
def report_stuba(disp_league, disp_gender, disp_year, 
                    disp_team, disp_pair, disp_player,
                    comp_l1_checked, disp_comp_l1,
                    comp_l2_checked, disp_comp_l2,
                    comp_l3_checked, disp_comp_l3,
                    date_checked, disp_start_date, disp_end_date,
                    scout, explain_text
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
    fbhe_vector = fbhe( m_ppr_df, disp_player, 'att', True )
    fbhe_table.at[0,'All'] = fbhe_vector[0]  # fbhe
    fbhe_table.at[1,'All'] = fbhe_vector[1]  # attacks
    fbhe_table.at[2,'All'] = fbhe_vector[2]  # errors
    fbhe_table.at[3,'All'] = fbhe_vector[3]  # attempts
    fbhe_table.at[4,'All'] = fbhe_vector[4]  # confidence interval
    fbhe_table.at[5,'All'] = fbhe_vector[5]  # URL

    # calculate for zones 1 - 5
    column = ['Zone 1','Zone 2','Zone 3','Zone 4','Zone 5']
    for i in [1,2,3,4,5]:
      fbhe_vector = fbhe( m_ppr_df[m_ppr_df['att_src_zone_net']==i], disp_player, 'att', True )
      fbhe_table.at[0,column[i-1]] = fbhe_vector[0]  # fbhe
      fbhe_table.at[1,column[i-1]] = fbhe_vector[1]  # attacks
      fbhe_table.at[2,column[i-1]] = fbhe_vector[2]  # errors
      fbhe_table.at[3,column[i-1]] = fbhe_vector[3]  # attempts
      fbhe_table.at[4,column[i-1]] = fbhe_vector[4]  # confidence interval
      fbhe_table.at[5,column[i-1]] = fbhe_vector[5]  # URL
 
    # now create the markdown text to return
    fbhe_return = pd.DataFrame.to_markdown(fbhe_table, index = False )
  else:
    fbhe_return = "No Data Found"

  
  return fbhe_return, ' ', ' '

@anvil.server.callable
def player_srv_strategies( disp_league, disp_gender, disp_year, pair_a, disp_player, disp_team ):
  '''

  Create a report that determines the best serve strategies against pair_b, sered by pair_a
  list these in a table with URL's, then draw them on a chart

  But this is set for hte matchup reports for hte format.

  Modified this ... shows serving strategies for only one player using all data.  player12 should be  the number 1 or 2. to determine whcih of hte two players in the pair to display
  
  '''
  # parameters
  num_srv_strategies = 10

  disp_player1, disp_player2 = pair_players(pair_a)
  if (disp_player != disp_player1) and (disp_player != disp_player2):
    print(f"player_srv_stratgies: disp player not found: {disp_player}, {pair_a}")
    
  # open my data sources
  # fetch the pair_data and pair_data_stats files
  player_data_df, player_stats_df = get_player_data( disp_league, disp_gender, disp_year)

  # get the row for disp_player
  if player_data_df['player'].isin([disp_player]).any():
    player_index = player_data_df.loc[ (player_data_df['player'] == disp_player)].index[0]
  else:
    return 'Player not found in player data:'+disp_player, '', '', '','', '', '','', '', '','', '', '',''
    
  # call to get the serve stretegy table
  matchup_df = player_45_serves(disp_league, disp_gender, disp_year, disp_player, disp_team)

  # sort by FBHE, take the top num_srv_Strategies
  matchup_df = matchup_df.sort_values(by='fbhe', ascending=True)
  matchup_df = matchup_df.head(num_srv_strategies)

  # make the 6 plots:
  # first, the dotted serve lines
  # max and min for the color bar to the right
  cmax = player_stats_df['fbhe_mean']+2*player_stats_df['fbhe_stdev']
  cmin = player_stats_df['fbhe_mean']-2*player_stats_df['fbhe_stdev']

  # make x,y for serve lines:
  x11 = [0.5,0.5,0.5]
  x12 = [0,4,8]
  x31 = [4,4,4]
  x51 = [7.5,7.5,7.5]
  y1 = [-8,-8,-8]
  y2 = [8,8,8]

  # create the data to plot, 6 total plots
  pass_x = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass_y = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass1_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass3_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
  pass5_val = [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]

  # loop thru the pair_data to populate the pass&&_val data to plot
  # now, loop thru the list for serves from zone 1
  index = 0
  x = 8.8
  for i in [1,2,3,4,5]:  # j is along the net
    x = x - 1.6
    y = 2.4
    for j in ['c','d','e']: # k is depth+
      y = y + 1.6
      fbhe1_var = 'fbhe_1_'+str(i)+str(j)
      fbhe3_var = 'fbhe_3_'+str(i)+str(j)
      fbhe5_var = 'fbhe_5_'+str(i)+str(j)
      pass1_val[index] = player_data_df.loc[player_index,fbhe1_var]
      pass3_val[index] = player_data_df.loc[player_index,fbhe3_var]
      pass5_val[index] = player_data_df.loc[player_index,fbhe5_var]
      pass_x[index] = x
      pass_y[index] = y
      index = index + 1

  # =============   create zone 1 plots (b1 and b2), add any serve stretegy lines. ==============================

  # ------ From Zone 1 to Player B1 ------
  fig1, ax1 = plt.subplots(figsize=(10,18)) # create a figure
  plot_court_background(fig1,ax1)
  ax1.plot( [x11, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax1.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass1_val, vmin=cmin, vmax=cmax, cmap='PiYG' ) 

  # now limit the matchup_df to this player and zone, then loop thru plotting hte line
  tmp_srv_strat_df = matchup_df[ (matchup_df['rcv_player'] == disp_player) ]
  #print(f"tmp srv strategy, srv from 1, player {player_b1}, number of serves: {tmp_srv_strat_df.shape[0]}")
  tmp_srv_strat_df = tmp_srv_strat_df[ tmp_srv_strat_df['srv_fr'] == '1' ]
  #print(f"tmp srv strategy, srv from 1, player {player_b1}, number of serves: {tmp_srv_strat_df.shape[0]}")
  if tmp_srv_strat_df.shape[0] > 0:
    for srv_index, srv in tmp_srv_strat_df.iterrows():
      # plot hte line, find our 'to' point
      zone_depth = 0 if (srv['srv_to_depth'] == 'c') else 1 if (srv['srv_to_depth'] == 'd') else 2
      zone_index = int((float(srv['srv_to_net'])-1)*3 + zone_depth)
      # line direction, to calculate dx and dy for the arrow
      distance = math.dist( [x11[0], y1[0]], [ pass_x[zone_index], pass_y[zone_index] ])
      if distance != 0:
        dx = ((pass_x[zone_index] - x11[0])/distance)*0.1
        dy = ((pass_y[zone_index] - y1[0])/distance)*0.1
      else:
        dx = 0
        dy = 0
      #print(f"Plotting serve strategy player:{player_b1} from:{srv['srv_fr']} to: {srv['srv_to_net']},{srv['srv_to_depth']}, index: {zone_index}, x and y: {pass_x[zone_index]},{pass_y[zone_index]} dx, dy {dx},{dy}")
      ax1.plot( [x11[0], pass_x[zone_index] ], [ y1[0] , pass_y[zone_index] ], c='black', linestyle='solid', linewidth = 4 )
      ax1.arrow(  pass_x[zone_index], pass_y[zone_index] , dx, dy, shape='full', lw=4, length_includes_head=True, head_width=.10, head_length = .25, color = 'black')

  #print(f"creating plot image for {player_b1}")
  plot_1 = anvil.mpl_util.plot_image()
  plt.close()

  # ======================. create zone 3 plots (b1 and b2), add any serve stretegy lines  ================================
  # ------ From Zone 3 to Player B1 ------
  fig1, ax1 = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig1,ax1)
  ax1.plot( [x31, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax1.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass3_val, vmin=cmin, vmax=cmax, cmap='PiYG' ) 

  # now limit the matchup_df to this player and zone, then loop thru plotting hte line
  tmp_srv_strat_df = matchup_df[ (matchup_df['rcv_player'] == disp_player) ]
  #print(f"tmp srv strategy, srv from 3, player {player_b1}, number of serves: {tmp_srv_strat_df.shape[0]}")
  tmp_srv_strat_df = tmp_srv_strat_df[ tmp_srv_strat_df['srv_fr'] == '3' ]
  #print(f"tmp srv strategy, srv from 3, player {player_b1}, number of serves: {tmp_srv_strat_df.shape[0]}")
  if tmp_srv_strat_df.shape[0] > 0:
    for srv_index, srv in tmp_srv_strat_df.iterrows():
      # plot hte line, find our 'to' point
      zone_depth = 0 if (srv['srv_to_depth'] == 'c') else 1 if (srv['srv_to_depth'] == 'd') else 2
      zone_index = (int(srv['srv_to_net'])-1)*3 + zone_depth
      zone_index = int(zone_index)
      # line direction, to calculate dx and dy for the arrow
      distance = math.dist( [x31[0], y1[0]], [ pass_x[zone_index], pass_y[zone_index] ])
      if distance != 0:
        dx = ((pass_x[zone_index] - x31[0])/distance)*0.1
        dy = ((pass_y[zone_index] - y1[0])/distance)*0.1
      else:
        dx = 0
        dy = 0
      #print(f"Plotting serve strategy player:{player_b1} from:{srv['srv_fr']} to: {srv['srv_to_net']},{srv['srv_to_depth']}, index: {zone_index}, x and y: {pass_x[zone_index]},{pass_y[zone_index]} ")
      ax1.plot( [x31[0], pass_x[zone_index] ], [ y1[0] , pass_y[zone_index] ], c='black', linestyle='solid', linewidth = 4 )
      ax1.arrow( pass_x[zone_index], pass_y[zone_index] , dx, dy, shape='full', lw=4, length_includes_head=True, head_width=.10, head_length = .25, color = 'black')

  #print(f"creating plot image for {player_b1}")
  plot_3 = anvil.mpl_util.plot_image()
  plt.close()

  # ==================. create zone 5 plots (b1 and b2), add any serve stretegy lines. ================================
  # ------ From Zone 5 to Player B1 ------
  fig1, ax1 = plt.subplots(figsize=(10,18)) # cretae a figure
  plot_court_background(fig1,ax1)
  ax1.plot( [x51, x12], [y1, y2], c='0.75', linestyle='dashed', linewidth =2.5 )
  ax1.scatter( pass_x, pass_y, s = np.full(len(pass_x),4000), c=pass5_val, vmin=cmin, vmax=cmax, cmap='PiYG' ) 

  # now limit the matchup_df to this player and zone, then loop thru plotting teh line
  tmp_srv_strat_df = matchup_df[ (matchup_df['rcv_player'] == disp_player) ]
  #print(f"tmp srv strategy, srv from 5, player {player_b1}, number of serves: {tmp_srv_strat_df.shape[0]}")
  tmp_srv_strat_df = tmp_srv_strat_df[ tmp_srv_strat_df['srv_fr'] == '5' ]
  #print(f"tmp srv strategy, srv from 5, player {player_b1}, number of serves: {tmp_srv_strat_df.shape[0]}")
  if tmp_srv_strat_df.shape[0] > 0:
    for srv_index, srv in tmp_srv_strat_df.iterrows():
      # plot hte line, find our 'to' point
      zone_depth = 0 if (srv['srv_to_depth'] == 'c') else 1 if (srv['srv_to_depth'] == 'd') else 2
      zone_index = (float(srv['srv_to_net'])-1)*3 + zone_depth
      zone_index = int(zone_index)
      # line direction, to calculate dx and dy for the arrow
      distance = math.dist( [x51[0], y1[0]], [ pass_x[zone_index], pass_y[zone_index] ])
      if distance != 0:
        dx = ((pass_x[zone_index] - x51[0])/distance)*0.1
        dy = ((pass_y[zone_index] - y1[0])/distance)*0.1
      else:
        dx = 0
        dy = 0
      #print(f"Plotting serve strategy player:{player_b1} from:{srv['srv_fr']} to: {srv['srv_to_net']},{srv['srv_to_depth']}, index: {zone_index}, x and y: {pass_x[zone_index]},{pass_y[zone_index]} ")
      ax1.plot( [x51[0], pass_x[zone_index] ], [ y1[0] , pass_y[zone_index] ], c='black', linestyle='solid', linewidth = 4 )
      ax1.arrow( pass_x[zone_index], pass_y[zone_index], dx, dy, shape='full', lw=4, length_includes_head=True, head_width=.10, head_length = .25, color = 'black')

  #print(f"creating plot image for {player_b1}")
  plot_5 = anvil.mpl_util.plot_image()
  plt.close()

  # convert matchup_df to markdown to display
  srv_strategies_table = pd.DataFrame.to_markdown( matchup_df, index = False)
 
  # the return here needs to be 8 mkdn's followed by 6 plots
  return srv_strategies_table, '',  '',  '',  '',  '',  '',  '', plot_1,  ' ', plot_3, ' ', plot_5, ' '
