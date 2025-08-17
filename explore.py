from nba_api.stats.endpoints import leaguedashplayerstats
import pandas as pd

# Get most recent season stats (2024-25 for example)
data = leaguedashplayerstats.LeagueDashPlayerStats(season="2024-25").get_data_frames()[0]
data.head(10)
