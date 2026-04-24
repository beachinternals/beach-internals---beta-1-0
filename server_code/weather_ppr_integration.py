# ============================================================================
# Weather Integration for PPR Conversion
# Add this function to btd_ppr_conversion.py
# ============================================================================

from logger_utils import log_info, log_error, log_critical, log_debug
from weather_integration import get_or_create_weather
import traceback


def add_weather_to_ppr(ppr_df, btd_row):
  """
    Fetch weather and add weather_id and weather data to all rows in ppr_df
    
    This function is called during BTD→PPR conversion, after all PPR data
    has been calculated but before storing the CSV file.
    
    Args:
        ppr_df (DataFrame): PPR dataframe with all calculated data
        btd_row: Row from btd_files table containing venue_id, date, match_time
        
    Returns:
        DataFrame: ppr_df with weather columns populated:
            - weather_id: Link to weather_data table
            - temperature_f: Temperature in Fahrenheit
            - wind_speed_mph: Wind speed in mph
            - wind_gust_mph: Wind gust speed in mph
            - humidity_percent: Humidity percentage
            - uv_index: UV index (may be None from Open-Meteo archive)
        
    Notes:
        - All rows in ppr_df get the SAME weather data (one match = one weather)
        - Backwards compatible: skips if venue_id/match_time are missing/empty
        - Caches weather: won't re-fetch if weather already exists
        - Weather data is denormalized into PPR for easy analysis
    """

  log_debug(f"add_weather_to_ppr called for file: {btd_row['filename']}")

  # Get required fields from btd_row
  venue_name = btd_row['venue_name']
  match_date = btd_row['date']
  match_time = btd_row['match_time']

  # Check if weather is already in the PPR dataframe
  already_has_weather = (
    'weather_id' in ppr_df.columns and
    ppr_df['weather_id'].notna().any() and
    (ppr_df['weather_id'] != '').any()
  )

  if already_has_weather:
    log_debug(f"Weather already present in PPR for {btd_row['filename']}, skipping fetch")
    return ppr_df

  # No weather yet — check if we have enough info to fetch it
  if not venue_name or not match_date or not match_time:
    log_info(f"Skipping weather for {btd_row['filename']}: "
             f"venue_name={venue_name}, date={match_date}, time={match_time} "
             f"(missing/empty fields)")
    ppr_df['weather_id']       = None
    ppr_df['temperature_f']    = None
    ppr_df['wind_speed_mph']   = None
    ppr_df['wind_gust_mph']    = None
    ppr_df['humidity_percent'] = None
    return ppr_df

  # Validate match_time format
  valid_times = ["8am-12pm", "12pm-4pm", "4pm-8pm"]
  if match_time not in valid_times:
    log_error(f"Invalid match_time '{match_time}' for {btd_row['filename']}")
    ppr_df['weather_id']       = None
    ppr_df['temperature_f']    = None
    ppr_df['wind_speed_mph']   = None
    ppr_df['wind_gust_mph']    = None
    ppr_df['humidity_percent'] = None
    return ppr_df

  # All good — fetch weather
  try:
    ...  # rest of the existing fetch code stays exactly the same
  # Validate: catches None, "", and missing columns
  # This makes it backwards compatible with old files
    if not venue_name or not match_date or not match_time:
      log_info(f"Skipping weather for {btd_row['filename']}: "
             f"venue_name={venue_name}, date={match_date}, time={match_time} "
             f"(missing/empty fields - backwards compatible)")

      # Add empty weather columns (pandas will create if they don't exist)
      ppr_df['weather_id'] = None
      ppr_df['temperature_f'] = None
      ppr_df['wind_speed_mph'] = None
      ppr_df['wind_gust_mph'] = None
      ppr_df['humidity_percent'] = None
      #ppr_df['uv_index'] = None

      return ppr_df

    # Validate match_time format
    valid_times = ["8am-12pm", "12pm-4pm", "4pm-8pm"]
    if match_time not in valid_times:
      log_error(f"Invalid match_time '{match_time}' for {btd_row['filename']} "
              f"(must be one of: {valid_times})")
      ppr_df['weather_id'] = None
      ppr_df['temperature_f'] = None
      ppr_df['wind_speed_mph'] = None
      ppr_df['wind_gust_mph'] = None
      ppr_df['humidity_percent'] = None
      #ppr_df['uv_index'] = None
      return ppr_df

    # Get or create weather (checks cache first!)
    try:
      log_debug(f"Attempting to get/create weather: venue_name={venue_name}, "
              f"date={match_date}, time={match_time}")

      weather_id = get_or_create_weather(venue_name, match_date, match_time)

      if weather_id:
        # Success! Now get the weather data to copy fields
        from anvil.tables import app_tables
        weather_row = app_tables.weather_data.get_by_id(weather_id)

        if weather_row:
          # Add weather_id to ALL rows in ppr_df
          ppr_df['weather_id'] = weather_id
  
          # Add weather data fields to ALL rows
          ppr_df['temperature_f'] = weather_row['temperature_f']
          ppr_df['wind_speed_mph'] = weather_row['wind_speed_mph']
          ppr_df['wind_gust_mph'] = weather_row['wind_gust_mph']
          ppr_df['humidity_percent'] = weather_row['humidity_percent']
          #ppr_df['uv_index'] = weather_row['uv_index']

          log_info(f"✓ Weather ID {weather_id} and data added to {len(ppr_df)} PPR rows: "
                 f"{btd_row['filename']}, venue={venue_id}, "
                 f"date={match_date}, time={match_time}, "
                 f"temp={weather_row['temperature_f']}°F, wind={weather_row['wind_speed_mph']}mph")
        else:
          # Shouldn't happen, but handle it
          log_error(f"Weather ID {weather_id} returned but could not fetch weather row")
        ppr_df['weather_id'] = weather_id
        ppr_df['temperature_f'] = None
        ppr_df['wind_speed_mph'] = None
        ppr_df['wind_gust_mph'] = None
        ppr_df['humidity_percent'] = None
        #ppr_df['uv_index'] = None
    else:
      # Weather fetch failed
      ppr_df['weather_id'] = None
      ppr_df['temperature_f'] = None
      ppr_df['wind_speed_mph'] = None
      ppr_df['wind_gust_mph'] = None
      ppr_df['humidity_percent'] = None
      #ppr_df['uv_index'] = None
      log_error(f"Failed to fetch weather for {btd_row['filename']}: "
                f"venue_name={venue_name}, date={match_date}, time={match_time}")

  except Exception as e:
    # Catch any errors and log them
    log_error(f"Exception in add_weather_to_ppr for {btd_row['filename']}: {str(e)}")
    log_error(traceback.format_exc())
    ppr_df['weather_id'] = None
    ppr_df['temperature_f'] = None
    ppr_df['wind_speed_mph'] = None
    ppr_df['wind_gust_mph'] = None
    ppr_df['humidity_percent'] = None
    #ppr_df['uv_index'] = None

  return ppr_df


# ============================================================================
# INTEGRATION INSTRUCTIONS
# ============================================================================
"""
To integrate this into btd_ppr_conversion.py:

1. Add import at top:
   from weather_ppr_integration import add_weather_to_ppr

2. In generate_ppr_files_not_background(), add after calc_tactic():
   
   if calc_ppr:
       # ... existing code ...
       ppr_df = calc_tactic(ppr_df)
       
       # NEW: Add weather to PPR
       ppr_df = add_weather_to_ppr(ppr_df, flist_r)
       
       # 4) Error check the ppr file
       ppr_df, no_errors, error_string = error_check_ppr(ppr_df)
       
       # ... rest of code ...

3. Also update flist_r.update() to store weather_id in btd_files:
   
   # Get weather_id from first row (all rows have same weather)
   weather_id = ppr_df['weather_id'].iloc[0] if 'weather_id' in ppr_df.columns else None
   
   flist_r.update(
       ppr_data=ppr_media,
       error_str=error_string,
       no_errors=no_errors,
       ppr_file_date=datetime.now(),
       weather_id=weather_id,  # NEW: Store in btd_files
       weather_fetched=True if weather_id else False  # NEW: Track if fetched
   )
"""