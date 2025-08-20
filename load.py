from config import engine
from sqlalchemy.dialects.postgresql import insert

def load_table(df, table_name: str):
    """Append dataframe into target Postgres table with upsert where needed."""
    if table_name in ("player", "team"):
        # Use upsert for dimension tables
        df.to_sql(table_name, engine, if_exists="append", index=False, method=upsert_method)
    else:
        # Use plain multi-row inserts for fact tables (e.g., stats history)
        df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")

    print(f"✅ Loaded {len(df)} rows into {table_name}")


def upsert_method(table, conn, keys, data_iter):
    """
    Custom upsert method for Postgres.
    Uses ON CONFLICT DO UPDATE for deduplicating rows by PK.
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data)

    # Exclude the PK (don't try to update it)
    update_dict = {c.name: c for c in stmt.excluded if c.name not in ["player_id", "team_id"]}

    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=[keys[0]],  # assumes first col in df is the PK
        set_=update_dict
    )

    conn.execute(upsert_stmt)