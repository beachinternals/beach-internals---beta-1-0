import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import pandas as pd
import io

# This is a server module. It runs on the Anvil server,

# Functions in this library
#
#   fbhe( ppr_df, disp_player):
#
#   get_ppr_data( disp_league, disp_gender, disp_year, disp_team, scout ): 

 
def fbhe( ppr_df, disp_player):
  # pass this a query of rows, figures the FBHE for the display player as the attacker
  # initialize the vector
  fbhe_list = [ 0.0, 0, 0, 0, 0, " " ]    # FBHE

  # limit to attacks by our player
  print(f"fbhe funct: ppr_df shape:{ppr_df.shape}")
  if ppr_df.shape[0] == 0:      # Then no data passed!
    fbhe_list = [ 0.0, 0, 0, 0, 0, " No Data Available " ]
  else:
    ppr_df = ppr_df[ppr_df['att_player']==disp_player]

    # to build the video link, need a quick loop over rows:
    video_list = [*range(0,ppr_df.shape[0],1)]
    #print(f"video list: {video_list}")
    video_btd_id = ""
    video_link = ""
    i = 0
    for index,r in ppr_df.iterrows():
      if r['video_id'] != video_btd_id:
        # build a new link
        #print(f"start new link, video_btd_id:{video_btd_id}, Lenght: {len(video_btd_id)}")
        video_link = video_link + ")" if len(video_btd_id) != 0 else video_link
        video_list[i] = " [Game "+str(i)+"](https://app.balltime.com/video/"+r['video_id']+"?actionIds="+str(r['serve_action_id'])
        video_list[i] = video_list[i] + ',' + str(r['pass_action_id']) if r['pass_action_id'] != 0 else video_list[i]
        video_list[i] = video_list[i] + ',' + str(r['set_action_id']) if r['set_action_id'] != 0 else video_list[i]
        video_list[i] = video_list[i] + ',' + str(r['att_action_id']) if r['att_action_id'] != 0 else video_list[i]
        video_list[i] = video_list[i] + ',' + str(r['dig_action_id']) if r['dig_action_id'] != 0 else video_list[i]
        video_list[i] = video_list[i] 
        video_link = video_link+ video_list[i]
        #print(f"New Link i: {i} Video Link: {video_link}")
        i += 1
      elif r['video_id'] == video_btd_id:
        # add on to the current video list
        video_list[i] = str(video_list[i]) + ',' + str(r['serve_action_id']) if r['serve_action_id'] != 0 else video_list[i]
        video_list[i] = video_list[i] + ',' + str(r['pass_action_id']) if r['pass_action_id'] != 0 else video_list[i]
        video_list[i] = video_list[i] + ',' + str(r['set_action_id']) if r['set_action_id'] != 0 else video_list[i]
        video_list[i] = video_list[i] + ',' + str(r['att_action_id']) if r['att_action_id'] != 0 else video_list[i]
        video_list[i] = video_list[i] + ',' + str(r['dig_action_id']) if r['dig_action_id'] != 0 else video_list[i]
        video_link = video_link+ video_list[i]
        #print(f"Add to existing Link i: {i}, Video Link: {video_link}")
      
      video_btd_id = r['video_id']

    video_link = video_link + ")" if len(video_link) != 0 else video_link
    if "No Video Id" in video_link:     # in case we have old data with no video id
      video_link = ""
    
    #print(f"player :{disp_player}, ppr df size:{ppr_df.shape}")
    fbhe_list[3] = ppr_df.shape[0]  # number of attempts
    fbhe_list[1] = ppr_df[ppr_df.point_outcome == "FBK"].shape[0] # kills
    fbhe_list[2] = ppr_df[ppr_df.point_outcome == "FBE"].shape[0] # errors
    fbhe_list[0] = ( fbhe_list[1] - fbhe_list[2]) / fbhe_list[3] if fbhe_list[3] != 0 else 0  # fbhe
    fbhe_list[0] = float("{:.3f}".format(fbhe_list[0]))
    fbhe_list[4] = 0 # need to calculate 95% confidence interval
    fbhe_list[5] = video_link
    print(f"fbhe Funct: fbhe_list:{fbhe_list}")

  return fbhe_list
  
def get_ppr_data( disp_league, disp_gender, disp_year, disp_team, scout ):  
  #
  # fetch the appropriate ppr table(s) from the ppr_csv table given the league and team, and if scout data

  no_data = True
  ppr_csv_row = app_tables.ppr_csv_tables.get( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = disp_team
      ) )

  if ppr_csv_row:
    m_ppr_df =  pd.read_csv(io.BytesIO( ppr_csv_row['ppr_csv'].get_bytes()))
    ppr_for_team_found = True
  else:
    m_ppr_df = [" "]
    print('No Team Rows Found')
    ppr_for_team_found = False

  if scout:
    # now look for the scout data (league wide public data) and merge the two
    # print(f"League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}, Player:{disp_player}")
    ppr_scout_row = app_tables.ppr_csv_tables.get( 
      q.all_of(
        league = disp_league,
        gender = disp_gender,
        year = disp_year,
        team = "Scout"
      ) 
    )

    if ppr_scout_row:
      scout_ppr_df =  pd.read_csv(io.BytesIO( ppr_scout_row['ppr_csv'].get_bytes()))
      if ppr_for_team_found:
        m_ppr_df = pd.concat([m_ppr_df,scout_ppr_df])
      else:
        m_ppr_df = scout_ppr_df
      print(f'Scout DB Found:{scout_ppr_df.shape}')
    else:
      print('No Scout Rows Found')


  return m_ppr_df

def ppr_df_limit( m_ppr_df, 
                          comp_l1_checked, disp_comp_l1, 
                          comp_l2_checked, disp_comp_l2, 
                          comp_l3_checked, disp_comp_l3, 
                          date_checked, disp_start_date, disp_end_date
                         ):
  #
  # take an imput ppr_df, and limit it by competition level and date
  if comp_l1_checked:
    m_ppr_df = m_ppr_df[ m_ppr_df['comp_l1'] ==  disp_comp_l1 ]
    print(f"Limitiing by Comp l1:{disp_comp_l1}, Size:{m_ppr_df.shape}")
  if comp_l2_checked:
    m_ppr_df = m_ppr_df[ m_ppr_df['comp_l2'] == disp_comp_l2 ]
    print(f"Limitiing by Comp l2:{disp_comp_l2}, Size:{m_ppr_df.shape}")
  if comp_l3_checked:
    m_ppr_df = m_ppr_df[ m_ppr_df['comp_l3'] == disp_comp_l3 ]
    print(f"Limitiing by Comp l3:{disp_comp_l3}, Size:{m_ppr_df.shape}")
  if date_checked:
     m_ppr_df['game_date'] = pd.to_datetime(m_ppr_df['game_date'])
    m_ppr_df['game_date'] = m_ppr_df['game_date'].dt.date
    m_ppr_df = m_ppr_df.loc[(m_ppr_df['game_date'] >= disp_start_date) & (m_ppr_df['game_date'] <= disp_end_date) ]
    print(f"Limitiing by Dates:{disp_start_date},{disp_end_date}")
    
  return m_ppr_df