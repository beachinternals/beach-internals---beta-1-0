import anvil.server
import anvil.email
import anvil.google.auth, anvil.google.drive, anvil.google.mail
from anvil.google.drive import app_files
import anvil.users
import anvil.tables.query as q
import anvil.tables as tables
import anvil.tables.query as q
from anvil.tables import app_tables
from PIL import Image
from io import BytesIO
import base64
import pandas as pd
import uuid
import json
import datetime

@anvil.server.callable
def generate_and_store_report():
  # Generate images
  images = []
  for color in ['red', 'green', 'blue']:
    img = Image.new('RGB', (100, 100), color=color)
    buffered = BytesIO()
    img.save(buffered, format="PNG")
    img_str = base64.b64encode(buffered.getvalue()).decode()
    images.append(f"data:image/png;base64,{img_str}")

    # Generate dataframes
    dataframes = [
      pd.DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25, 30, 35],
        'City': ['New York', 'London', 'Paris']
      }),
      pd.DataFrame({
        'Product': ['Apple', 'Banana', 'Orange'],
        'Price': [1.0, 0.5, 0.75],
        'Stock': [100, 150, 200]
      })
    ]
  # Convert dataframes to JSON for storage
  df_json = [df.to_json() for df in dataframes]

  # Generate labels
  labels = [
    "First Image Set",
    "Personnel Data",
    "Inventory Data",
    "Second Image Set"
  ]

  # Generate second set of images
  more_images = []
  for color in ['yellow', 'purple', 'orange']:
    img = Image.new('RGB', (100, 100), color=color)
    buffered = BytesIO()
    img.save(buffered, format="PNG")
    img_str = base64.b64encode(buffered.getvalue()).decode()
    more_images.append(f"data:image/png;base64,{img_str}")

    # Generate unique report ID
    report_id = str(uuid.uuid4())

  # Store in temporary Data Table row
  app_tables.report_data.add_row(
    report_id=report_id,
    images=json.dumps(images),
    dataframes=json.dumps(df_json),
    labels=json.dumps(labels),
    more_images=json.dumps(more_images),
    created_at=datetime.datetime.now()
  )

  return report_id

@anvil.server.callable
def get_report_data(report_id):
  row = app_tables.report_data.get(report_id=report_id)
  if not row:
    return None
    return {
      'images': json.loads(row['images']),
      'dataframes': [pd.read_json(df) for df in json.loads(row['dataframes'])],
      'labels': json.loads(row['labels']),
      'more_images': json.loads(row['more_images'])
    }


    import anvil.server

@anvil.server.callable
def test_now():
  return datetime.datetime.now()