import time
import json
import os
from nba_api.stats.endpoints import CommonPlayerInfo

CACHE_FILE = "player_heights.json"

def fetch_player_heights(player_ids, delay=0.3):
  """Fetch player heights from NBA API with throttling."""
  heights = {}
  for i, pid in enumerate(player_ids, 1):
    try:
      response = CommonPlayerInfo(player_id=pid).get_normalized_dict()
      height = response["CommonPlayerInfo"][0]["HEIGHT"]
      heights[pid] = height
      print(f"[{i}/{len(player_ids)}] {pid} → {height}")
    except Exception as e:
      print(f"Error fetching height for player_id {pid}: {e}")
      heights[pid] = None # fallback
    time.sleep(delay) # respect API rate limits
  return heights

def load_heights_from_cache():
  if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as f:
      print(f"Reading player heights from cache {CACHE_FILE}")
      return json.load(f)
  return {}

def save_heights_to_cache(heights):
  with open(CACHE_FILE, "w") as f:
    json.dump(heights, f, indent=2)