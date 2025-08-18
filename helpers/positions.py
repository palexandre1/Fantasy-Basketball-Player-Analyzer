import time
import json
import os
from nba_api.stats.endpoints import CommonPlayerInfo

CACHE_FILE = "player_positions.json"

def fetch_player_positions(player_ids, delay=0.3):
    """Fetch player positions from NBA API with throttling."""
    positions = {}
    for i, pid in enumerate(player_ids, 1):
        try:
            response = CommonPlayerInfo(player_id=pid).get_normalized_dict()
            position = response["CommonPlayerInfo"][0]["POSITION"]
            positions[pid] = position
            print(f"[{i}/{len(player_ids)}] {pid} → {position}")
        except Exception as e:
            print(f"Error fetching position for player_id {pid}: {e}")
            positions[pid] = None # fallback
        time.sleep(delay) # respect API rate limits
    return positions

def load_positions_from_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            print(f"Reading player positions from cache {CACHE_FILE}")
            return json.load(f)
    return {}

def save_positions_to_cache(positions):
    with open(CACHE_FILE, "w") as f:
        json.dump(positions, f, indent=2)