import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.
#
# To allow anvil.server.call() to call functions here, we mark
# them with @anvil.server.callable.
# Here is an example - you can replace it with your own:
#
# @anvil.server.callable
# def say_hello(name):
#   print("Hello, " + name + "!")
#   return 42
#

 
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
  
 