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
                for event in data:
                    event_details = {key: event[key] for key in
                    ['duel', 'block', 'clearance', 'interception',
                     'dribble', 'shot', 'substitution', 'foul_won', 
                     'foul_committed', 'goalkeeper', 'bad_behaviour',
                     'pass', '50_50', 'tactics', 'injury_stoppage',
                     'ball_receipt', 'carry'] if key in event}                 

                    yield {
                        'event_id': event["id"],  
                        'match_id': match_id,  # Convert match_id to int if it's not already
                        'index': event.get('index'),
                        'period': event.get('period'),
                        'timestamp': event.get('timestamp'),
                        'minute': event.get('minute'),
                        'second': event.get('second'),
                        'type_name': event['type']['name'],
                        'possession': event.get('possession'),
                        'possession_team_id': event['possession_team']['id'],
                        'play_pattern': event['play_pattern']['name'],
                        'team_id': event['team']['id'],
                        'player_id': event.get('player', {}).get('id'),
                        'position_name': event.get('position', {}).get('name'),
                        'location': event.get('location'),  # Assuming location is a list [x, y]
                        'duration': event.get('duration'),
                        'under_pressure': event.get('under_pressure', False),
                        'counterpress': event.get('counterpress', False),
                        'events_details': json.dumps(event_details)  # Store the event details as JSON
                    }

def insert_json_data(json_data_generator, connection_params):
    """Insert event data into the database."""
    insert_query = """
    INSERT INTO events (
        event_id, match_id, index, period, timestamp,
        minute, second, type_name, possession, possession_team_id,
        play_pattern, team_id, player_id, position_name, location,
        duration, under_pressure, counterpress, events_details
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (event_id) DO NOTHING;
    """
    with psycopg2.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            for item in json_data_generator:
                location = item['location']
                pg_location = f"({location[0]}, {location[1]})" if location else None
                cur.execute(insert_query, (
                    item['event_id'], item['match_id'], item['index'], item['period'],
                    item['timestamp'], item['minute'], item['second'], item['type_name'],
                    item['possession'], item['possession_team_id'], item['play_pattern'],
                    item['team_id'], item['player_id'], item['position_name'], pg_location,
                    item['duration'], item['under_pressure'], item['counterpress'], item['events_details']
                ))
        conn.commit()

# Directory containing JSON files
directory_path = 'D:\\Carleton university\\2024W\\COMP 3005\\Project\\data\\data\\events'

# Load JSON data
json_data_generator = load_json_files(directory_path, target_match_ids)

# Insert the JSON data into the database
insert_json_data(json_data_generator, conn_params)