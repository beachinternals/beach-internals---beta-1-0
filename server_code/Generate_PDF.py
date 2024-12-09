import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.server
import anvil.pdf as pdfRenderer

# This is a server module. It runs on the Anvil server,
# rather than in the user's browser.

@anvil.server.callable
def render_pdf_rpt():
  print("in Render pdf rpt")
  media_object = anvil.pdf.render_form('Homepage.PDF_Rpt')
  return media_object