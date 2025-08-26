import pandas as pd
from nba_api.stats.static import teams as static_teams
import os
from helpers.convertMinutesToFloat import convert_minutes_to_float

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

def transform_players(df):
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

def transform_player_stats(df_raw: pd.DataFrame, season: str) -> pd.DataFrame:
    """Transform raw NBA stats dataframe to match Postgres schema for player_game_stats."""

    # Select and rename columns to match schema
    column_mapping = {
        "PLAYER_ID": "player_id",
        "TEAM_ID": "team_id",
        "GP": "games_played",
        "MIN": "minutes",
        "PTS": "points",
        "DREB": "defensive_rebounds",
        "OREB": "offensive_rebounds",
        "REB": "total_rebounds",
        "AST": "assists",
        "STL": "steals",
        "BLK": "blocks",
        "TOV": "turnovers",
        "FGA": "fga",
        "FGM": "fgm",
        "FG_PCT": "fg_pct",
        "FTA": "fta",
        "FTM": "ftm",
        "FT_PCT": "ft_pct",
        "FG3A": "three_pta",
        "FG3M": "three_ptm",
        "FG3_PCT": "three_pct",
        "PF": "fouls",
        "NBA_FANTASY_PTS": "fantasy_points"
    }

    df = df_raw[list(column_mapping.keys())].rename(columns=column_mapping)
    df["minutes"] = df["minutes"].apply(convert_minutes_to_float)

    # Ensure correct data types (important for Postgres load)
    int_cols = [
        "player_id", "team_id", "games_played", "points", "defensive_rebounds",
        "offensive_rebounds", "total_rebounds", "assists", "steals",
        "blocks", "turnovers", "fga", "fgm", "three_pta",
        "three_ptm", "fta", "ftm", "fouls"
    ]
    float_cols = ["minutes", "fg_pct", "ft_pct", "three_pct", "fantasy_points"]

    df[int_cols] = df[int_cols].astype(int)
    df[float_cols] = df[float_cols].astype(float)

    df['season'] = season

    return df

def aggregate_player_game_stats(df):
    # --- Aggregate player_game_stats across teams per season ---
    sum_cols = [
        "games_played", "minutes", "points", "defensive_rebounds", "offensive_rebounds",
        "total_rebounds", "assists", "steals", "blocks", "turnovers",
        "fga", "fgm", "three_pta", "three_ptm", "fta", "ftm", "fouls",
        "fantasy_points"
    ]

    # Group by player and season, sum counting stats
    df_agg = df.groupby(["player_id", "season"], as_index=False)[sum_cols].sum()

    # Recompute percentages correctly
    df_agg["fg_pct"] = (df_agg["fgm"] / df_agg["fga"]).fillna(0).round(2)
    df_agg["ft_pct"] = (df_agg["ftm"] / df_agg["fta"]).fillna(0).round(2)
    df_agg["three_pct"] = (df_agg["three_ptm"] / df_agg["three_pta"]).fillna(0).round(2)

    return df_agg

def deduplicate_players(df):
    df.drop_duplicates(subset="player_id", inplace=True)
    return df


def normalize_games_df(games_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw GameHeader dataframe into desired format with a surrogate key."""
    games_df = games_df[["GAME_ID", "GAME_DATE_EST", "SEASON", "HOME_TEAM_ID", "VISITOR_TEAM_ID"]].copy()

    home = games_df[["GAME_ID", "GAME_DATE_EST", "SEASON", "HOME_TEAM_ID"]].copy()
    home.rename(columns={
        "GAME_DATE_EST": "game_date",
        "SEASON": "season_id",
        "HOME_TEAM_ID": "team_id",
        "GAME_ID": "game_id"
    }, inplace=True)

    away = games_df[["GAME_ID", "GAME_DATE_EST", "SEASON", "VISITOR_TEAM_ID"]].copy()
    away.rename(columns={
        "GAME_DATE_EST": "game_date",
        "SEASON": "season_id",
        "VISITOR_TEAM_ID": "team_id",
        "GAME_ID": "game_id"
    }, inplace=True)

    normalized = pd.concat([home, away], ignore_index=True)

    normalized["game_id"] = normalized["game_id"].astype(str)
    normalized["game_date"] = pd.to_datetime(normalized["game_date"]).dt.date
    normalized["season_id"] = normalized["season_id"].astype(str)
    normalized["team_id"] = normalized["team_id"].astype(int)

    normalized["game_team_id"] = normalized["game_id"].astype(str) + "_" + normalized["team_id"].astype(str)
    normalized = normalized[["game_team_id", "game_id", "game_date", "season_id", "team_id"]]

    return normalized

def normalize_player_game_stats(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw boxscore dataframe to match player_game_stats schema
    and compute fantasy points:"""

    df = raw_df.copy()

    # Rename columns to match schema
    df.rename(columns={
        "PLAYER_ID": "player_id",
        "GAME_ID": "game_id",
        "TEAM_ID": "team_id",
        "MIN": "minutes",
        "FGM": "fgm",
        "FGA": "fga",
        "FG_PCT": "fg_pct",
        "FG3M": "fg3m",
        "FG3A": "fg3a",
        "FG3_PCT": "fg3_pct",
        "FTM": "ftm",
        "FTA": "fta",
        "FT_PCT": "ft_pct",
        "OREB": "oreb",
        "DREB": "dreb",
        "REB": "reb",
        "AST": "ast",
        "STL": "stl",
        "BLK": "blk",
        "TO": "turnovers",
        "PF": "pf",
        "PTS": "pts",
        "PLUS_MINUS": "plus_minus"
    }, inplace=True)

    df["minutes"] = df["minutes"].apply(convert_minutes_to_float)

    # Fill missing numeric values with 0
    numeric_cols = ["fgm","fga","fg_pct","fg3m","fg3a","fg3_pct","ftm","fta","ft_pct",
                    "oreb","dreb","reb","ast","stl","blk","turnovers","pf","pts","plus_minus"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Compute fantasy points
    df["fantasy_points"] = (
        df["pts"] + 1.2*df["reb"] + 1.5*df["ast"] + 3*df["stl"] + 3*df["blk"] - 1*df["turnovers"]
    )

    # Keep only columns in schema
    schema_cols = ["player_id", "game_id", "team_id", "minutes", "fgm","fga","fg_pct","fg3m","fg3a",
                   "fg3_pct","ftm","fta","ft_pct","oreb","dreb","reb","ast","stl","blk",
                   "turnovers","pf","pts","plus_minus","fantasy_points"]

    df = df[schema_cols]
    df["game_id"] = df["game_id"].astype(str).str.zfill(10)
    df["game_team_id"] = df["game_id"] + "_" + df["team_id"].astype(str)

    return df

