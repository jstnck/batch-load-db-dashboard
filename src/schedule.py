"""Scrapes NBA schedule from basketballreference.com"""
import os
import sys
import time
import logging
import requests
from bs4 import BeautifulSoup

# sys.path.append("../../db")
from postgres import Database

# db
DB_NAME = 'prod'
DB_TABLE = 'schedule'
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_SCHEMA = os.environ.get("DB_SCHEMA")

# choose which months to scrape (reg. season = october to april)
MONTHS = ["october", "november", "december", "january", "february", "march", "april"]

def scrape_schedule(months: list):
    """Scrapes the schedule from bball reference
       for a list of months"""
    full_schedule = []
    schedule_base_url = "https://www.basketball-reference.com/leagues/NBA_2023_games-"

    request_headers = {'User-Agent': 'Mozilla/5.0'}

    for month in months:
        # get html response    
        url = schedule_base_url + month + ".html"
        while response is None:
            try:
                response = requests.get(url, headers = request_headers)
            except requests.HTTPError as error:
                logging.log(f"[!] Exception caught: {error}")
                time.sleep(2)
        
        # sleep to avoid website rate limiting
        time.sleep(0.5)
        
        # parse soup
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table')
        table_body = table.find('tbody')
        rows = table_body.find_all('tr')
        
        # add rows to full schedule
        for row in rows:
            date_col = row.find('th')
            date = date_col.text.strip()
            data_cols = row.find_all('td')
            data = [ele.text.strip() for ele in data_cols]
            full_schedule.append([date] + [ele for ele in data])
        
    return full_schedule

def check_num_of_games(full_schedule):
    """Check that you have collected the correct number of games"""
    if len(full_schedule) != 82*30/2:
        raise(Exception(
            f"{len(full_schedule)} games have been scraped, it should be 1230"
            ))

def truncate_table(db, db_schema, db_table):
    """Truncates the raw table"""
    sql_truncate_table = f"TRUNCATE TABLE {db_schema}.{db_table}"
    db.execute_query(sql_truncate_table)

def insert_to_db(db, data):
    """Insert into postgres"""
    for d in data:
        string_row = "','".join([str(ele) for ele in d])
        sql_insert_row = f"""
        INSERT INTO {DB_SCHEMA}.{DB_TABLE}(
            gdate,
            start_time,
            away_team,
            away_pts,
            home_team,
            home_pts,
            box_score_url,
            overtime,
            attendance,
            arena,
            notes) 
        VALUES(
            '{string_row}'
        )
        """
        db.execute_query(sql_insert_row)

def main():
    """Scrape schedule data, validate num games, and 
       insert into postgres"""
    data = scrape_schedule(MONTHS)

    if len(MONTHS) == 7:
        check_num_of_games(data)
    
    db = Database(DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD)
    db.truncate_table(DB_SCHEMA, DB_TABLE)
    db.insert_to_db(data)
    db.close_connection()

if __name__ == "__main__":
    main()
