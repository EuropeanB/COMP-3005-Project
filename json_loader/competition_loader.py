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

def load_json_files(directory):
    """Generator to load JSON objects from all files in the specified directory."""
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r', encoding='utf-8') as file:
                yield from json.load(file)  

def insert_json_data(json_data, connection_params):
    """Insert JSON data into the database."""
    insert_query = """
    INSERT INTO competitions (
        competition_id, season_id, country_name, competition_name,
        competition_gender, competition_youth, competition_international,
        season_name, match_updated, match_updated_360, match_available_360,
        match_available
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (competition_id, season_id) DO NOTHING;
    """
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            for item in json_data:
                cur.execute(insert_query, (
                    item["competition_id"], item["season_id"], item["country_name"],
                    item["competition_name"], item["competition_gender"],
                    item["competition_youth"], item["competition_international"],
                    item["season_name"], item.get("match_updated"),
                    item.get("match_updated_360"), item.get("match_available_360"),
                    item.get("match_available")
                ))

# Directory containing JSON files
directory_path = 'D:\\Carleton university\\2024W\\COMP 3005\\Project\\data\\data'

# Load JSON data
json_data_generator = load_json_files(directory_path)

# Insert the JSON data into the database
insert_json_data(json_data_generator, conn_params)