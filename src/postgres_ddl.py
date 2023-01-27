"""Postgres DDL for basketball-refernce.com schedule"""
import os
import sys
from postgres import Database


DB_NAME = os.environ.get("DB_NAME")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_SCHEMA = os.environ.get("DB_SCHEMA")

def main():
    """main function"""
    # Create initial connection to new/empty Postgres instance

    # TODO: have database name brought in as env variable, so it
    # can be set by ConfigMap or cli parameter

    sql_create_table_players = f"""CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.players(
        id uuid NOT NULL DEFAULT gen_random_uuid(),
        team VARCHAR,
        players JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )"""

    sql_create_table_schedule = f"""CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.schedule(
        id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        gdate VARCHAR,
        start_time VARCHAR,
        away_team VARCHAR,
        away_pts VARCHAR,
        home_team VARCHAR,
        home_pts VARCHAR,
        box_score_url VARCHAR,
        overtime VARCHAR,
        attendance VARCHAR,
        arena VARCHAR,
        notes VARCHAR,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )"""

    db = Database('postgres', DB_HOST, DB_PORT, DB_USER, DB_PASSWORD)
    db.create_database(DB_NAME)
    db.close_connection()
    db = Database(DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD)
    db.execute_query(sql_create_table_players)
    db.execute_query(sql_create_table_schedule)

    db.close_connection()



if __name__ == "__main__":
    main()
