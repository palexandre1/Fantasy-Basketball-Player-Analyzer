def convert_minutes_to_float(min_str):
  """Convert 'MM:SS' string to float minutes."""
  if isinstance(min_str, str) and ":" in min_str:
      mins, secs = min_str.split(":")
      val = int(mins) + int(secs) / 60
      return round(val, 2)
  try:
      return round(float(min_str), 2)  # already numeric (edge case)
  except:
      return 0.0  # handle missing values safely