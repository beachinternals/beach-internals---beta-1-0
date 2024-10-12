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
from server_functions import fbhe

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
                    date_checked, disp_start_date, disp_end_date
                    ):
  
  # return a fbhe of a given player
  # we will return the text as markdown to be displayed in a rich text box in the client
  # the difference here is that we combine the scout file with the team file to query aginst.
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

  # first the team file:
  # filter the master_ppr_data file to this player and team and league)
  print(f"FBHE_Scout_Query: League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}, Player:{disp_player}")
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = disp_team
      ) )

  no_data = True
  if ppr_csv_row:
    m_ppr_df =  pd.read_csv(io.BytesIO( ppr_csv_row['ppr_csv'].get_bytes()))
    ppr_for_team_found = True
    no_data = False
  else:
    m_ppr_df = [ " "]
    print('No Team Rows Found')
    ppr_for_team_found = False

  # first nwo the scout file::
  # filter the master_ppr_data file to this player and team and league)
  # print(f"League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}, Player:{disp_player}")
  ppr_scout_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = "Scout"
      ) )

  if ppr_scout_row:
    scout_ppr_df =  pd.read_csv(io.BytesIO( ppr_scout_row['ppr_csv'].get_bytes()))
    if ppr_for_team_found:
      m_ppr_df = pd.concat([m_ppr_df,scout_ppr_df])
    else:
      m_ppr_df = scout_ppr_df
      no_data = False
    print(f'Scout DB Found:{scout_ppr_df.shape}')
  else:
    print('No Scout Rows Found')
    no_data = True

  print(f"master scout data frame:{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")

  # now, narrow the data by competition level and dates
  if comp_l1_checked:
    m_ppr_df = m_ppr_df[ m_ppr_df['comp_l1'] ==  disp_comp_l1 ]
    print(f"Limitiing by Comp l1:{disp_comp_l1}, Size:{m_ppr_df.shape}")
  if comp_l2_checked:
    m_ppr_df = m_ppr_df[ m_ppr_df['comp_l2'] == disp_comp_l2 ]
    print(f"Limitiing by Comp l2:{disp_comp_l2}, Size:{m_ppr_df.shape}")
  if comp_l1_checked:
    m_ppr_df = m_ppr_df[ m_ppr_df['comp_l3'] == disp_comp_l3 ]
    print(f"Limitiing by Comp l3:{disp_comp_l3}, Size:{m_ppr_df.shape}")
  if date_checked:
    print(f"Checking types. disp_date:{disp_start_date}, Game_date:{m_ppr_df['game_date']}")
    m_ppr_df = m_ppr_df.query[ ( m_ppr_df['game_date'] >= disp_start_date ) & ( m_ppr_df['game_date'] <= disp_end_date ) ]
    print(f"Limitiing by Dates:{disp_start_date},{disp_end_date}, Size:{m_ppr_df.shape}")
    
  print(f"master scout data frame (after filter):{m_ppr_df.shape}, display player:{disp_player} m ppr df 0:{m_ppr_df.shape[0]}")
    
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
 