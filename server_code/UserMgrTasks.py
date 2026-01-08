import anvil.secrets
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server

# This is a server module. It runs on the Anvil server,
@anvil.server.callable
def check_user_team():
  # check if the user has a team assigned, maybe double check that all temas are valid
  team_list = ['INTERNALS','VISITOR','BALLTIME',
              'FSU',
              'LMU', 'STETSON'
              ]

  user_row = anvil.users.get_user()
  if user_row:
    current_team = anvil.users.get_user()['team']
    if current_team is not None:
      current_team = current_team.strip()
      current_team = current_team.upper()
    if current_team in team_list:
      # all is good
      #print(f"Current user is on a valid team: {current_team}")
      user_row['team'] = current_team  #save it back, jsut in case, now it should be upper case and trimmed.
    elif not current_team:
      # send me an email about this
      anvil.email.send(from_name = "Beach Internals Support", 
                 to = "beachinternals@gmail.com",
                 subject = "New User Assigned to Visitor",
                 text = "A new user has been assigned to team Visitor :"+user_row['email'])
    else:
      # so we have an issue
      #print(f"Appears we have a team with an invalid team: {current_team}")
      user_row['team'] = 'Visitor'
      # send me an email about this
      anvil.email.send(from_name = "Beach Internals Support", 
                 to = "beachinternals@gmail.com",
                 subject = "User with Invalide Team",
                 text = "A user has an Invlaid Team :"+current_team+', email:'+user_row['email'])

    # last thing .. if any defaults are blank, set them to our default value ...
    user_row['team'] = "Visitor" if not user_row['team'] else user_row['team']
    user_row['def_league'] = "NCAA" if not user_row['def_league'] else user_row['def_league']
    user_row['def_gender'] = "W" if not user_row['def_gender'] else user_row['def_gender']
    user_row['def_year'] = "2024" if not user_row['def_year'] else user_row['def_year']

    # last, last thing, if they are a visitor, only subscriptions are ncaa m/w 2024.  This is in subscriptions DB, but set default here
    if user_row['team'] == 'VISITOR':
      user_row['def_league'] = 'NCAA'
      user_row['def_gender'] = "W"
      user_row['def_year'] = '2024'
  

  return True