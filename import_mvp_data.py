import json
import mysql.connector

# Define the function to map JSON to DB columns
def map_json_to_db_columns(year, player_name, player_stats):
    def convert_percentage(percentage_str):
        return float(percentage_str.strip('%')) / 100.0 if percentage_str else None

    def safe_float(value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def safe_int(value):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    return {
        'year': safe_int(year),
        'player_name': player_name,
        'rank': safe_int(player_stats.get("Voting - Rank").replace('T', '')) if player_stats.get("Voting - Rank") else None,
        'age': safe_int(player_stats.get("Voting - Age")),
        'team': player_stats.get("Voting - Tm"),
        'first': safe_float(player_stats.get("Voting - First")),
        'pts_won': safe_float(player_stats.get("Voting - Pts Won")),
        'pts_max': safe_float(player_stats.get("Voting - Pts Max")),
        'share': safe_float(player_stats.get("Voting - Share")),
        'g': safe_int(player_stats.get("Voting - G")),
        'mp': safe_float(player_stats.get("Voting - MP")),
        'pts': safe_float(player_stats.get("Voting - PTS")),
        'trb': safe_float(player_stats.get("Voting - TRB")),
        'ast': safe_float(player_stats.get("Voting - AST")),
        'stl': safe_float(player_stats.get("Voting - STL")) if player_stats.get("Voting - STL") else None,
        'blk': safe_float(player_stats.get("Voting - BLK")) if player_stats.get("Voting - BLK") else None,
        'fg_pct': convert_percentage(player_stats.get("Voting - FG%")),
        'three_pct': convert_percentage(player_stats.get("Voting - 3P%")),
        'ft_pct': convert_percentage(player_stats.get("Voting - FT%")),
        'ws': safe_float(player_stats.get("Voting - WS")),
        'ws_per_48': safe_float(player_stats.get("Voting - WS/48")),
    }

# Load JSON data from file
json_file_path = 'mvp_data.json'  # Replace with your JSON file path
with open(json_file_path, 'r') as file:
    data = json.load(file)

# Flatten the JSON data
flattened_data = []
for year, players in data.items():
    for player_name, stats in players.items():
        row = map_json_to_db_columns(year, player_name, stats)
        flattened_data.append(row)

# Set up database connection (replace with your credentials)
db_config = {
    'host': 'localhost',
    'user': 'root',
    'passwd': 'Drake!143',
    'database': 'mvp_data'
}

# Connect to the MySQL database
db_connection = mysql.connector.connect(**db_config)
cursor = db_connection.cursor()

# SQL insert statement
insert_query = """
    INSERT INTO stats (`year`, `player_name`, `rank`, `age`, `team`, `first`, `pts_won`, `pts_max`, `share`, `g`, `mp`, `pts`, `trb`, `ast`, `stl`, `blk`, `fg_pct`, `three_pct`, `ft_pct`, `ws`, `ws_per_48`)
    VALUES (%(year)s, %(player_name)s, %(rank)s, %(age)s, %(team)s, %(first)s, %(pts_won)s, %(pts_max)s, %(share)s, %(g)s, %(mp)s, %(pts)s, %(trb)s, %(ast)s, %(stl)s, %(blk)s, %(fg_pct)s, %(three_pct)s, %(ft_pct)s, %(ws)s, %(ws_per_48)s);
"""

# Insert the data into the MySQL table
for row in flattened_data:
    cursor.execute(insert_query, row)

# Commit the transaction
db_connection.commit()

# Close the connection
cursor.close()
db_connection.close()
