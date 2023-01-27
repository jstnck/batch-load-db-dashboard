"""Scrapes NBA players for 2023 from basketballreference.com"""
import os
import sys
import time
import json
import logging
import requests
from bs4 import BeautifulSoup

# sys.path.append("/app/db")
from postgres import Database

# db
DB_NAME = 'prod'
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_SCHEMA = os.environ.get("DB_SCHEMA")
DB_TABLE = os.environ.get("DB_TABLE")

def get_teams(db: Database) -> list:
    """query a list of teams from postgres and return it"""
    sql_get_teams = "select team from ingest.teams_seed"
    results = db.execute_query_return(sql_get_teams)
    results = [res[0] for res in results]
    results.sort()
    return results

def scrape_players(teams: list) -> dict:
    """scrapes player info by respective team"""
    players = {}
    teams_base_url = "https://www.basketball-reference.com/teams/"
    request_headers = {'User-Agent': 'Mozilla/5.0'}

    for team in teams:
        # get html response
        url = teams_base_url + team + "/2023.html"
        while response is None:
            try:
                response = requests.get(url, headers = request_headers)
            except requests.HTTPError as error:
                logging.log(f"[!] Exception caught: {error}")
                time.sleep(2)
        
        # sleep to avoid website rate limiting
        time.sleep(1)

        # parse soup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        table = soup.find('table')
        table_body = table.find('tbody')
        player_rows = table_body.find_all('tr')
        
        team_players = tuple()
        for row in player_rows:
            player_info = {}
            number = row.find('th').text.strip()
            if number:
                player_info.update({"No.": number})

            data_cols = row.find_all('td')
            data = [ele.text.strip() for ele in data_cols]

            player_info.update({
                "Player": data[0],
                "Pos": data[1],
                "Ht": data[2],
                "Wt": data[3],
                "BirthDate": data[4],
                "Country": data[5],
                "Exp": data[6],
                "College": data[7]
            })
            team_players = (*team_players, player_info)
        players.update({team: team_players})
    return players

def truncate_table(db, db_schema, db_table):
    """Truncates the raw table"""
    sql_truncate_table = f"TRUNCATE TABLE {db_schema}.{db_table}"
    db.execute_query(sql_truncate_table)

def players_to_json(data):
    """Format json strings for postgres insert"""
    js_string = json.dumps(data)
    return js_string.replace("'", "''")

def insert_to_db(db, team, data):
    """Inserts data into postgres"""
    sql_insert_data = f"""
                      INSERT INTO {DB_SCHEMA}.{DB_TABLE}(team, data)
                      VALUES('{team}', '{data}')
                      """
    db.execute_query(sql_insert_data)
    logging.info(f"{team} inserted to pg")

def main():
    """Get a list of teams from postgres, scrape player data by team,
       insert player data into postgres"""
    db = Database(DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD)
    teams = get_teams(db)

    scraped_player_data = scrape_players(teams)
    
    if len(scraped_player_data.keys()) != 30:
        raise Exception(f"""'teams' dict length is {len(teams.keys())},
                            should be 30""")

    truncate_table(db, DB_SCHEMA, DB_TABLE)

    for team in scraped_player_data:
        players_js = players_to_json(scraped_player_data.get(team))
        logging.info("Inserting '{team}' data into db")
        insert_to_db(db, team, players_js)

    db.close_connection()

if __name__ == "__main__":
    main()
