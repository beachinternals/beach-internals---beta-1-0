import anvil.secrets
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
from datetime import datetime, timedelta

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

@anvil.server.callable
def call_chk_user_logins():
  task = anvil.server.launch_background_task('chk_user_logins')
  return task

# background task to check for user login's the last 24 hours and email me the list
@anvil.server.background_task
def chk_user_logins():

  # Get the current datetime
  now = datetime.now()

  # Calculate the time 24 hours ago
  twenty_four_hours_ago = now - timedelta(days=1)
  
  # check for logins in the last 24 hours
  mess_text = 'Beach Internals - Users logging in the this site between '+str(twenty_four_hours_ago)+" and " + str(now)+"\n"+"\n"
  print(f"twenty four housrs ago:{twenty_four_hours_ago} Type: {type(twenty_four_hours_ago)}")
  for tmp_r in app_tables.users.search(email='spccoach@gmail.com'):
    print(f"email:{tmp_r['email']}, last login: {tmp_r['last_login']}, last login type: {type(tmp_r['last_login'])}")
           
  for user_r in app_tables.users.search( last_login = q.greater_than(twenty_four_hours_ago) ):
    mess_text = mess_text + "User :"+user_r['email'] + " logged in at :"+str(user_r['last_login'])+"\n"

  # no send this in a email to Internals
  internals_email = 'beachinternals@gmail.com'
  print(mess_text)

  email_status = anvil.email.send(to=internals_email,from_address="no-reply",subject='Beach Internals - User Login History',text=mess_text)

  return email_status
    


  