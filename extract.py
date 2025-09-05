from nba_api.stats.static import players
from nba_api.stats.endpoints import leaguedashplayerstats, scoreboardv2, boxscoretraditionalv2
from config import CURRENT_SEASON
from functools import lru_cache
from helpers.positions import load_positions_from_cache, fetch_player_positions, save_positions_to_cache
from helpers.heights import load_heights_from_cache, fetch_player_heights, save_heights_to_cache
from transform import normalize_games_df, normalize_player_game_stats
import pandas as pd
from datetime import datetime, timedelta
import time
import random
import os
import numpy as np

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


def extract_games_by_season(
    start_date="2024-10-25",
    end_date="2025-04-15",
    cache_file="games_2024_25.csv"):
    """Extract NBA games for a season, normalize, and cache to disk."""

    if os.path.exists(cache_file):
        print(f"Loading cached season data from {cache_file}")
        return pd.read_csv(cache_file, dtype={"game_id": str})

    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    all_games = []

    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        try:
            scoreboard = scoreboardv2.ScoreboardV2(game_date=date_str)
            raw_games = scoreboard.get_data_frames()[0]
            if not raw_games.empty:
                normalized = normalize_games_df(raw_games)
                all_games.append(normalized)
                print(f"Fetched {len(normalized)} games for {date_str}")
            else:
                print(f"No games on {date_str}")
            time.sleep(random.uniform(0.5, 1.5))  # polite delay
        except Exception as e:
            print(f"Error fetching games for {date_str}: {e}")
            time.sleep(5)
        current += timedelta(days=1)

    if all_games:
        season_df = pd.concat(all_games, ignore_index=True)
        season_df.to_csv(cache_file, index=False)
        print(f"Saved normalized season data to {cache_file}")
        return season_df
    else:
        return pd.DataFrame(columns=["game_team_id", "game_id", "game_date", "season_id", "team_id"])


def extract_player_game_stats_for_game(game_id: str, cache_dir="player_game_cache") -> pd.DataFrame:
    """
    Fetch per-player stats for a single game and cache to CSV.
    """
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, f"{game_id}.csv")

    # Resume from cache and skip empty CSVs
    if os.path.exists(cache_file):
        df = pd.read_csv(cache_file, dtype={"game_id": str})
        if df.empty:
            print(f"[CACHE] Previous fetch for {game_id} was empty, skipping...")
            return pd.DataFrame()
        print(f"[CACHE] Loading cached stats for {game_id}")
        return df

    max_retries = 5
    delay = 2
    for attempt in range(max_retries):
        try:
            box = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id, timeout=60)
            raw_dfs = box.get_data_frames()
            if not raw_dfs:
                print(f"No resultSet for {game_id}, skipping...")
                return pd.DataFrame()

            df = raw_dfs[0]  # Player stats
            normalized = normalize_player_game_stats(df)  # normalization function
            normalized.to_csv(cache_file, index=False)
            print(f"[FETCHED] Stats for {game_id}")
            # Polite random delay
            time.sleep(random.uniform(2, 5))
            return normalized

        except Exception as e:
            print(f"[RETRY {attempt+1}] Error fetching {game_id}: {e}")
            time.sleep(delay)
            delay *= 2  # Exponential backoff

    # After all retries failed
    print(f"[FAILED] Could not fetch stats for {game_id} after {max_retries} attempts")
    return pd.DataFrame(columns=[
        "player_id","game_id","team_id","minutes","fgm","fga","fg_pct","fg3m","fg3a","fg3_pct",
        "ftm","fta","ft_pct","oreb","dreb","reb","ast","stl","blk","turnovers","pf","pts",
        "plus_minus","fantasy_points"
    ])



def extract_player_game_stats_for_season(game_ids: list, cache_dir="player_game_cache") -> pd.DataFrame:
    """
    Loop over all games in a season, extract player stats, normalize, and concatenate.
    """
    os.makedirs(cache_dir, exist_ok=True)
    all_stats = []

    for gid in game_ids:
        gid_str = str(gid).zfill(10)  # Ensure 10-digit string for NBA API
        stats = extract_player_game_stats_for_game(gid_str, cache_dir=cache_dir)

        # Ensure normalization even for cached files
        if not stats.empty:
            stats = normalize_player_game_stats(stats)
        all_stats.append(stats)

    if all_stats:
        return pd.concat(all_stats, ignore_index=True)
    else:
        return pd.DataFrame()