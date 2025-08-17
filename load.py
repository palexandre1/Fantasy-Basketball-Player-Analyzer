from config import engine

def load_table(df, table_name: str):
    """Append dataframe into target Postgres table."""
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"✅ Loaded {len(df)} rows into {table_name}")
