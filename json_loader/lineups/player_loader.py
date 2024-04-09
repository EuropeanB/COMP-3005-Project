import json
import psycopg2
import os

# Database connection parameters
conn_params = {
    "dbname": "Project",  
    "user": "postgres",  
    "password": "3526384",  
    "host": "localhost" ,
    "port": "3526"
}

def get_target_match_ids(connection_params, competition_ids, season_ids):
    """Query the database for match IDs belonging to the target competition and season IDs."""
    target_match_ids = set()
    query = """
    SELECT match_id FROM matches 
    WHERE competition_id = %s AND season_id = %s
    """
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            for comp_id in competition_ids:
                for season_id in season_ids:
                    cur.execute(query, (comp_id, season_id))
                    target_match_ids.update(str(match_id) for (match_id,) in cur.fetchall())
    return target_match_ids

target_match_ids = get_target_match_ids(conn_params, {'11', '2'}, {'4', '42', '90', '44'})

def load_json_files(directory, target_match_ids):
    """Load cards data from JSON files in the specified directory that are in the target match IDs."""
    for filename in os.listdir(directory):
        match_id = os.path.splitext(filename)[0]  # Get the match ID from filename
        if match_id in target_match_ids:
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r', encoding='utf-8') as file:
                data = json.load(file)
                for team in data:
                    for player in team.get('lineup', []):
                        country_name = player['country']['name'] if 'country' in player else None
                        yield {
                            'match_id': match_id,
                            'player_id': player['player_id'],
                            'player_name': player['player_name'],
                            'player_nickname': player['player_nickname'],
                            'jersey_number': player['jersey_number'],
                            'country_name': country_name,
                        } 

def insert_json_data(json_data_generator, connection_params):
    """Insert players data into the database."""
    insert_query = """
    INSERT INTO players (
        player_id, player_name, player_nickname, jersey_number, country_name
    ) VALUES (
        %s, %s, %s, %s, %s
    )
    ON CONFLICT (player_id) DO NOTHING;
    """
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            for item in json_data_generator:
                cur.execute(insert_query, (
                    item['player_id'], item['player_name'], item['player_nickname'],
                    item['jersey_number'], item['country_name']
                ))

# Directory containing JSON files
directory_path = 'D:\\Carleton university\\2024W\\COMP 3005\\Project\\data\\data\\lineups'

# Load JSON data
json_data_generator = load_json_files(directory_path, target_match_ids)

# Insert the JSON data into the database
insert_json_data(json_data_generator, conn_params)