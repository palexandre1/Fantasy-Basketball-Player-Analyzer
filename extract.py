from nba_api.stats.static import players
from nba_api.stats.endpoints import leaguedashplayerstats
from config import CURRENT_SEASON
from functools import lru_cache
from helpers.positions import load_positions_from_cache, fetch_player_positions, save_positions_to_cache
from helpers.heights import load_heights_from_cache, fetch_player_heights, save_heights_to_cache
import pandas as pd

@lru_cache(maxsize=1)
def extract_player_stats():
    """Extract stats for all active NBA players for the current season."""

    # Step 1: Get all active NBA players
    active_players = players.get_active_players()
    active_names = {p['full_name'] for p in active_players}  # use a set for fast lookup

    # Step 2: Pull all player stats (defaults to NBA)
    df = leaguedashplayerstats.LeagueDashPlayerStats(season=CURRENT_SEASON).get_data_frames()[0]

    # Step 3: Filter DataFrame to only include active NBA players
    df_nba = df[df['PLAYER_NAME'].isin(active_names)].reset_index(drop=True)

    return df_nba

def get_player_positions(player_ids, delay=0.3):
    """Load cached positions or fetch missing ones, then update cache."""
    cached_positions = load_positions_from_cache()
    missing_ids = [pid for pid in player_ids if str(pid) not in cached_positions]

    if missing_ids:
        print(f"Fetching {len(missing_ids)} missing player positions...")
        new_positions = fetch_player_positions(missing_ids, delay=delay)
        # Merge into cache
        cached_positions.update({str(pid): pos for pid, pos in new_positions.items()})
        save_positions_to_cache(cached_positions)
    return {int(pid): pos for pid, pos in cached_positions.items()}

def get_player_heights(player_ids, delay=0.3):
    """Load cached heights or fetch missing ones, then update cache."""
    cached_heights = load_heights_from_cache()
    missing_ids = [pid for pid in player_ids if str(pid) not in cached_heights]

    if missing_ids:
        print(f"Fetching {len(missing_ids)} missing player heights...")
        new_heights = fetch_player_heights(missing_ids, delay=delay)
        # Merge into cache
        cached_heights.update({str(pid): height for pid, height in new_heights.items()})
        save_heights_to_cache(cached_heights)
    return {int(pid): height  for pid, height in cached_heights.items()}