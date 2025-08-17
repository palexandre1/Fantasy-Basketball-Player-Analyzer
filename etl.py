from extract import extract_player_stats
from transform import transform_teams, transform_players
from load import load_table

def main():
    # Extract
    raw_df = extract_player_stats()
    print("✅ Extracted raw player stats")

    # Transform
    teams_df = transform_teams()
    players_df = transform_players(raw_df)
    print("✅ Transformed teams and players")

    # Load
    load_table(teams_df, "team")
    load_table(players_df, "player")
    print("✅ ETL completed")

if __name__ == "__main__":
    main()
