import pandas as pd
from nba_api.stats.static import teams as static_teams
import os

def transform_teams(cache_file="teams_static.csv"):
    """Transform NBA static teams data for Postgres."""
    if os.path.exists(cache_file):
        teams_df = pd.read_csv(cache_file)
        print(f"✅ Loaded team static data from {cache_file}")
        return teams_df
    else:
        all_teams = pd.DataFrame(static_teams.get_teams())
        all_teams = all_teams.rename(columns={
            "id": "team_id",
            "full_name": "name"
        })

        # Add missing columns with None if needed
        for col in ['conference', 'division']:
            if col not in all_teams.columns:
                all_teams[col] = None

        teams_df = all_teams[['team_id', 'name', 'abbreviation', 'conference', 'division']]
        teams_df.to_csv(cache_file, index=False)
        print(f"✅ Cached team static data to {cache_file}")

        return teams_df

def transform_players(df, cache_file="players_static.csv"):
    """
    Transform raw stats DataFrame into a players DataFrame for Postgres.
    Uses NBA static data to populate player attributes (position, height, weight, team_id).
    Only includes active players.
    Caches results to CSV for faster repeated runs.
    """
    # Extract basic stats
    player_stats = df[['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID']].drop_duplicates().copy()

    # Split first/last name safely
    name_split = player_stats['PLAYER_NAME'].str.split(' ', n=1, expand=True)
    player_stats['first_name'] = name_split[0]
    player_stats['last_name'] = name_split[1].fillna('')  # handle single-word names

    # Rename columns
    player_stats = player_stats.rename(columns={"PLAYER_ID": "player_id", "TEAM_ID": "team_id"})

    # Ensure missing columns exist
    for col in ['position', 'height', 'weight']:
        if col not in player_stats:
            player_stats[col] = None

    return player_stats[['player_id', 'first_name', 'last_name', 'position', 'team_id', 'height', 'weight']]

def player_game_stats(df):
