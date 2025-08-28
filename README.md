# Fantasy-Basketball-Player-Analyzer

## Overview  
This project extracts, transforms, and loads NBA player statistics into a PostgreSQL database to analyze player performance for fantasy basketball. The workflow allows you to fetch game-level stats, load them into structured tables, and run advanced SQL analysis to identify breakout players and fantasy value trends.  

## ETL Pipeline  

### 1. **Extract**  
- Data is fetched from the NBA API (via `nba_api` in Python).  
- For each game, player-level box scores are saved into CSV files.  
- Some games may return empty results; these are automatically skipped.  

### 2. **Transform**  
- CSVs are cleaned and normalized:  
  - Game IDs are cast to strings (`dtype={"game_id": str}`) to preserve leading zeros.  
  - Duplicate or empty files are skipped.  
  - Per-game stats are standardized into consistent column formats.  

### 3. **Load**  
- Cleaned data is loaded into a PostgreSQL database using `psycopg2` / `SQLAlchemy`.  
- Tables include:  
  - **player** – player metadata  
  - **player_per_game_stats** – game-level player stats  
- Optional: SQL Views can be created for summary stats, z-scores, and breakout player detection.  

## Analysis Examples  
Once data is in Postgres, you can query:  
- **Fantasy Points per Game (`avg_fp`)** – average production per player.  
- **Fantasy Points per Minute (`fp_per_min`)** – efficiency metric.  
- **Consistency (CV)** – volatility of performance using coefficient of variation.  
- **Z-Scores** – compare players against the population.  

## Tech Stack  
- **Python** (ETL scripts)  
- **PostgreSQL** (data warehouse)  
- **pgAdmin** (SQL GUI)  
- **nba_api** (data source)  
- **pandas** (data transformation)  

## How to Run  
1. Clone repo and install dependencies:
### Setup

#### Using Conda (Recommended)

Create the environment from `environment.yml`:

```bash
conda env create -f environment.yml
conda activate nba-etl
```
