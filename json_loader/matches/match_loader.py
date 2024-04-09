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

def load_json_files(directory, competition_ids, season_ids):
    """Generator to load JSON objects from all files in the specified directory that match the given competition and season IDs."""
    for dirpath, dirnames, filenames in os.walk(directory):
        dir_comp_id = os.path.basename(dirpath)  # Extract the competition_id from the directory name
        if dir_comp_id in competition_ids:
            for filename in filenames:
                file_season_id, _ = os.path.splitext(filename)  # Extract the season_id from the file name (removing .json)
                if file_season_id in season_ids:
                    filepath = os.path.join(dirpath, filename)
                    with open(filepath, 'r', encoding='utf-8') as file:
                        yield from json.load(file)

def insert_json_data(json_data, connection_params):
    """Insert JSON data into the database."""
    insert_query = """
    INSERT INTO matches (
        match_id, match_date, kick_off, competition_id,
        season_id, home_team_id, home_team_manager_id,
        away_team_id, away_team_manager_id, home_score, away_score,
        match_status, match_status_360, last_updated, last_updated_360,
        data_version, shot_fidelity_version, xy_fidelity_version,
        match_week, competition_stage, stadium_id, referee_id
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            for item in json_data:
                cur.execute(insert_query, (
                    item["match_id"], item["match_date"], item["kick_off"],
                    item["competition"]["competition_id"], item["season"]["season_id"],
                    item["home_team"]["home_team_id"], item["home_team"].get("managers", [{}])[0].get("id"),
                    item["away_team"]["away_team_id"], item["away_team"].get("managers", [{}])[0].get("id"),
                    item["home_score"], item["away_score"],
                    item["match_status"], item["match_status_360"],
                    item.get("last_updated"), item.get("last_updated_360"),
                    item.get("metadata", {}).get("data_version"),  item.get("metadata", {}).get("shot_fidelity_version"), 
                    item.get("metadata", {}).get("xy_fidelity_version"), item["match_week"],
                    item["competition_stage"]["id"], item.get("stadium", {}).get("id"), item.get("referee", {}).get("id") 
                ))

# Directory containing JSON files
directory_path = 'D:\\Carleton university\\2024W\\COMP 3005\\Project\\data\\data\\matches'

# Competition and Season IDs for the seasons we are interested in
target_competition_ids = {'11', '2'}  # As strings because directory names are strings
target_season_ids = {'4', '42', '90', '44'}  # Also as strings for file names

# Load JSON data for specific competition and season IDs
json_data_generator = load_json_files(directory_path, target_competition_ids, target_season_ids)

# Insert the JSON data into the database
insert_json_data(json_data_generator, conn_params)