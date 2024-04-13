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
    INSERT INTO stadiums (
        stadium_id, stadium_name, country_name
    ) VALUES (
        %s, %s, %s
    )
    ON CONFLICT (stadium_id) DO NOTHING;
    """
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            for item in json_data:
                stadium = item.get('stadium')
                if stadium is None:
                    continue
                cur.execute(insert_query, (
                    stadium.get("id"),
                    stadium.get("name"),                   
                    stadium.get("country", {}).get("name"),
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