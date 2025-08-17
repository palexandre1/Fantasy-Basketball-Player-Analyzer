from nba_api.stats.static import players
from nba_api.stats.endpoints import leaguedashplayerstats
from config import CURRENT_SEASON
import pandas as pd

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
