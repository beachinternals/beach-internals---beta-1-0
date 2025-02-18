from ._anvil_designer import pair_pdf_reportTemplate
from anvil import *
import anvil.server
import anvil.google.auth, anvil.google.drive
from anvil.google.drive import app_files
import anvil.users
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables


class pair_pdf_report(pair_pdf_reportTemplate):
  def __init__(
    self,
    report_title,
    player1,
    player2,
    table_data1,
    table_data2,
    table_data3,
    table_data4,
    table_data5,
    table_data6,
    table_data7,
    table_data8,
    box1_title,
    box2_title,
    box3_title,
    box4_title,
    box5_title,
    box6_title,
    box7_title,
    box8_title,
    filter_text,
    explain_text,
    **properties,
  ):
    # Set Form properties and Data Bindings.
    self.init_components(**properties)

    self.rpt_title.text = report_title
    self.player1_label.text = player1
    self.player2_label.text = player2
    
    self.rpt_disp_box1.content = table_data1
    self.rpt_disp_box2.content = table_data2
    self.rpt_disp_box3.content = table_data3
    self.rpt_disp_box4.content = table_data4
    self.rpt_disp_box5.content = table_data5
    self.rpt_disp_box6.content = table_data6
    self.rpt_disp_box7.content = table_data7
    self.rpt_disp_box8.content = table_data8

    self.box1_label.text = box1_title
    self.box2_label.text = box2_title
    self.box3_label.text = box3_title
    self.box4_label.text = box4_title
    self.box5_label.text = box5_title
    self.box6_label.text = box6_title
    self.box7_label.text = box7_title
    self.box8_label.text = box8_title
    
    self.explain_text.content = explain_text
    self.filter_text.content = filter_text
    self.filter_text.visible = False if report_title == 'Pair - Strengths & Weaknesses' else True

    # Any code you write here will run before the form opens.
