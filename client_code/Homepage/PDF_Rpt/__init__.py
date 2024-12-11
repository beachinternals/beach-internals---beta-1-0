from ._anvil_designer import PDF_RptTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
from anvil.tables import app_tables
import anvil.tables.query as q


class PDF_Rpt(PDF_RptTemplate):
  def __init__(self, email, text1, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)
    self.email_label.text = email
    self.text_label.text = text1

    # Any code you write here will run before the form opens.

    
