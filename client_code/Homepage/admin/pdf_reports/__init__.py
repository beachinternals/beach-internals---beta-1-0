from ._anvil_designer import pdf_reportsTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
import anvil.media


class pdf_reports(pdf_reportsTemplate):
  def __init__(self, **properties):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

  def outlined_button_1_click(self, **event_args):
    """This method is called when the button is clicked"""
    email = self.email_box.text
    text1 = self.text1_box.text
    
    
    if email and text1:
      alert(f'Thanks for registering! Your PDF is downloading and will be sent to {email}.')

      pdf = anvil.server.call('send_pdf_email', email, text1)
      anvil.media.download(pdf)

    else:
        alert('You have not completed all required fields')
    pass




