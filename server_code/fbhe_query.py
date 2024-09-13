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
  print(f"League:{disp_league}, Gender:{disp_gender}, Year:{disp_year}, Team:{disp_team}, Player:{disp_player}")
  ppr_csv_rows = app_tables.ppr_csv_tables.search( 
    q.all_of(
      league = disp_league,
      gender = disp_gender,
      year = disp_year,
      team = disp_team
      ) )

  for r in ppr_csv_rows:
    # pull out the csv file into a dataframe
    m_ppr_df =  pd.read_csv(io.BytesIO( r['ppr_csv'].get_bytes()))
  
  # Then calcualte the FBHE for all zones, store in a dataframe

  if len(ppr_csv_rows) != 0:
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
  #print(fbhe_return)
  
  return fbhe_return

def fbhe( ppr_df, disp_player):
  # pass this a query of rows, figures the FBHE for the display player as the attacker
  # initialize the vector
  fbhe_list = [ 0.0, 0, 0, 0, 0, " " ]    # FBHE

  # limit to attacks by our player
  ppr_df = ppr_df[ppr_df.att_player==disp_player]

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
  #print(f"player :{disp_player}, ppr df size:{ppr_df.shape}")
  fbhe_list[3] = ppr_df.shape[0]  # number of attempts
  fbhe_list[1] = ppr_df[ppr_df.point_outcome == "FBK"].shape[0] # kills
  fbhe_list[2] = ppr_df[ppr_df.point_outcome == "FBE"].shape[0] # errors
  fbhe_list[0] = ( fbhe_list[1] - fbhe_list[2]) / fbhe_list[3] if fbhe_list[3] != 0 else 0  # fbhe
  fbhe_list[0] = float("{:.3f}".format(fbhe_list[0]))
  fbhe_list[4] = 0 # need to calculate 95% confidence interval
  fbhe_list[5] = video_link
  #fbhe_list[5] = '[Video](https://app.balltime.com/video/'+['video_id']+'')'
  #fbhe_list[5] = '<a href="http://example.com/" target="_blank">example</a>' # can't get this to work

  # print(fbhe_list)

  return fbhe_list
  
  