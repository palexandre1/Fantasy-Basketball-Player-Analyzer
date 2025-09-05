from extract import extract_player_stats, get_player_positions, get_player_heights, extract_games_by_season, extract_player_game_stats_for_season
from transform import transform_players, transform_teams, transform_player_stats, aggregate_player_game_stats, deduplicate_players
from load import load_table
from config import CURRENT_SEASON

def main():
    ## --- Extract ---

    # # Step 1: Extract player total stats and teams
    # raw_df = extract_player_stats()
    # teams_df = transform_teams()
    # print("✅ Extract completed")

    # # --- Transform ---

    # # Step 2: Get unique player_ids
    # player_ids = raw_df["PLAYER_ID"].unique().tolist()

    # # Step 3: Fetch or load positions and height
    # positions = get_player_positions(player_ids)
    # heights = get_player_heights(player_ids)

    # # Step 4: Enrich DataFrame with positions and height
    # players_df = transform_players(raw_df)
    # players_df["position"] = players_df["player_id"].map(positions)
    # players_df["height"] = players_df["player_id"].map(heights)

    # # Step 5: Transform player stats to fit SQL DB
    # player_stats_df = transform_player_stats(raw_df, CURRENT_SEASON)

    # # Step 6: Deduplicate players and aggregate player stats across teams per season
    # players_df = deduplicate_players(players_df)
    # player_stats_df = aggregate_player_game_stats(player_stats_df)

    # Step 7: Extract all games for the current season (to link with player stats)
    games_df = extract_games_by_season()
    game_ids = games_df["game_id"].unique().tolist()
    print("✅ Game extraction completed")

    # Step 8: Extract player game stats for all games in the season
    player_per_game_stats_df = extract_player_game_stats_for_season(game_ids)
    print("✅ Player game stats extraction completed")

    print("✅ Transform completed")

    # --- Load ---
    # load_table(teams_df, "team")
    # load_table(players_df, "player")
    # load_table(player_stats_df, "player_game_stats" )
    # load_table(games_df, "games")
    load_table(player_per_game_stats_df, "player_per_game_stats")
    print("✅ Load completed")

    print("✅ ETL completed")

if __name__ == "__main__":
    main()
