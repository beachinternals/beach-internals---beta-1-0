def add_weather_to_ppr(ppr_df, btd_row):
  """
    Fetch weather and add to all rows in ppr_df
    Backwards compatible - skips if venue_id/time missing
    """

  venue_id = btd_row.get('venue_id')
  match_date = btd_row.get('date')
  match_time = btd_row.get('match_time')

  # Validates and skips if missing/empty
  if not venue_id or not match_date or not match_time:
    log_info("Skipping weather (backwards compatible)")
    ppr_df['weather_id'] = None
    return ppr_df

    # Get or create weather
  weather_id = get_or_create_weather(venue_id, match_date, match_time)

  # Add to ALL rows
  ppr_df['weather_id'] = weather_id

  return ppr_df