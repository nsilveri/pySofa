import psycopg2
from psycopg2 import sql, extras

def create_connection(dbname='postgres', user='postgres', password='password', host='localhost', port='5432'):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        return conn
    except Exception as e:
        print(f"Errore nella connessione al database: {e}")
        return None

def create_table(conn):
    create_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS matches (
        id BIGINT PRIMARY KEY,
        tournament TEXT,
        season TEXT,
        home_team TEXT,
        away_team TEXT,
        home_score TEXT,
        away_score TEXT,
        status TEXT,
        start_timestamp BIGINT,
        home_country TEXT,
        away_country TEXT
    );
    """)
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
        conn.commit()
        print("Tabella 'matches' creata o già esistente.")
    except Exception as e:
        print(f"Errore nella creazione della tabella: {e}")

def insert_matches(conn, events):
    insert_query = """
    INSERT INTO matches (id, tournament, season, home_team, away_team, home_score, away_score, status, start_timestamp, home_country, away_country)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """
    try:
        with conn.cursor() as cursor:
            for event in events:
                data = (
                    event['id'],
                    event['tournament']['name'],
                    event['season']['name'],
                    event['homeTeam']['name'],
                    event['awayTeam']['name'],
                    str(event.get('homeScore', {}).get('current', 'N/A')),
                    str(event.get('awayScore', {}).get('current', 'N/A')),
                    event['status']['description'],
                    event.get('startTimestamp'),
                    event['homeTeam']['country']['name'],
                    event['awayTeam']['country']['name']
                )
                cursor.execute(insert_query, data)
        conn.commit()
        print(f"Inseriti {len(events)} eventi nel database.")
    except Exception as e:
        print(f"Errore nell'inserimento dei dati: {e}")

def save_matches_to_db(events, db_config=None):
    if db_config is None:
        db_config = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'password',
            'host': 'localhost',
            'port': '5432'
        }
    conn = create_connection(**db_config)
    if conn:
        create_table(conn)
        insert_matches(conn, events)
        conn.close()
    else:
        print("Impossibile connettersi al database.")

def create_graphics_table(conn):
    # JSON table
    create_json_query = """
    CREATE TABLE IF NOT EXISTS match_graphics_json (
        match_id BIGINT PRIMARY KEY,
        graphics JSONB
    );
    """
    # Column table
    columns = ", ".join([f"possession_{i} FLOAT" for i in range(1, 91)])
    create_column_query = f"""
    CREATE TABLE IF NOT EXISTS match_graphics_column (
        match_id BIGINT PRIMARY KEY,
        {columns}
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_json_query)
            cursor.execute(create_column_query)
        conn.commit()
        print("Tabelle 'match_graphics_json' e 'match_graphics_column' create o già esistenti.")
    except Exception as e:
        print(f"Errore nella creazione delle tabelle graphics: {e}")

def insert_graphics(conn, match_id, graphics):
    print(f"Graphics data for match {match_id}: {graphics}")  # Debug
    # Insert into JSON
    insert_json_query = """
    INSERT INTO match_graphics_json (match_id, graphics)
    VALUES (%s, %s)
    ON CONFLICT (match_id) DO UPDATE SET graphics = EXCLUDED.graphics;
    """
    # Extract values for columns
    possession_values = {}
    if 'graphPoints' in graphics:
        for point in graphics['graphPoints']:
            minute = int(point.get('minute', 0))
            value = point.get('value', 0)
            if 1 <= minute <= 90:
                possession_values[f"possession_{minute}"] = value
    print(f"Possession values extracted: {possession_values}")  # Debug
    
    # Insert into columns
    columns = ", ".join([f"possession_{i}" for i in range(1, 91)])
    placeholders = ", ".join(["%s"] * 90)
    values = [possession_values.get(f"possession_{i}", None) for i in range(1, 91)]
    
    insert_column_query = f"""
    INSERT INTO match_graphics_column (match_id, {columns})
    VALUES (%s, {placeholders})
    ON CONFLICT (match_id) DO UPDATE SET
    """ + ", ".join([f"possession_{i} = EXCLUDED.possession_{i}" for i in range(1, 91)])
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_json_query, (match_id, extras.Json(graphics)))
            cursor.execute(insert_column_query, [match_id] + values)
        conn.commit()
        print(f"Grafici inseriti per match {match_id}.")
    except Exception as e:
        print(f"Errore nell'inserimento dei grafici per match {match_id}: {e}")

def save_graphics_to_db(match_id, graphics, db_config=None):
    if db_config is None:
        db_config = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'password',
            'host': 'localhost',
            'port': '5432'
        }
    conn = create_connection(**db_config)
    if conn:
        create_graphics_table(conn)
        insert_graphics(conn, match_id, graphics)
        conn.close()
    else:
        print("Impossibile connettersi al database per i grafici.")

def create_statistics_table(conn):
    # JSON table
    create_json_query = """
    CREATE TABLE IF NOT EXISTS match_statistics_json (
        match_id BIGINT PRIMARY KEY,
        statistics JSONB
    );
    """
    # Column table
    create_column_query = """
    CREATE TABLE IF NOT EXISTS match_statistics_column (
        match_id BIGINT PRIMARY KEY,
        possession_home FLOAT,
        possession_away FLOAT,
        shots_total_home INT,
        shots_total_away INT,
        shots_on_target_home INT,
        shots_on_target_away INT,
        passes_total_home INT,
        passes_total_away INT,
        pass_accuracy_home FLOAT,
        pass_accuracy_away FLOAT,
        fouls_home INT,
        fouls_away INT,
        yellow_cards_home INT,
        yellow_cards_away INT,
        red_cards_home INT,
        red_cards_away INT,
        corners_home INT,
        corners_away INT,
        offsides_home INT,
        offsides_away INT
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_json_query)
            cursor.execute(create_column_query)
        conn.commit()
        print("Tabelle 'match_statistics_json' e 'match_statistics_column' create o già esistenti.")
    except Exception as e:
        print(f"Errore nella creazione delle tabelle statistics: {e}")

def insert_statistics(conn, match_id, statistics):
    # Insert into JSON
    insert_json_query = """
    INSERT INTO match_statistics_json (match_id, statistics)
    VALUES (%s, %s)
    ON CONFLICT (match_id) DO UPDATE SET statistics = EXCLUDED.statistics;
    """
    # Extract values for columns
    stats_dict = {}
    if 'statistics' in statistics:
        for stat in statistics['statistics']:
            name = stat.get('name', '').lower().replace(' ', '_').replace('%', '')
            home_val = stat.get('home', '0')
            away_val = stat.get('away', '0')
            
            # Converti in tipi appropriati
            if 'possession' in name:
                stats_dict['possession_home'] = float(home_val.rstrip('%')) / 100 if '%' in home_val else float(home_val) / 100
                stats_dict['possession_away'] = float(away_val.rstrip('%')) / 100 if '%' in away_val else float(away_val) / 100
            elif 'shots_on_target' in name:
                stats_dict['shots_on_target_home'] = int(home_val)
                stats_dict['shots_on_target_away'] = int(away_val)
            elif 'total_shots' in name or ('shots' in name and 'total' in name):
                stats_dict['shots_total_home'] = int(home_val)
                stats_dict['shots_total_away'] = int(away_val)
            elif 'total_passes' in name:
                stats_dict['passes_total_home'] = int(home_val)
                stats_dict['passes_total_away'] = int(away_val)
            elif 'pass_accuracy' in name:
                stats_dict['pass_accuracy_home'] = float(home_val.rstrip('%')) / 100 if '%' in home_val else float(home_val)
                stats_dict['pass_accuracy_away'] = float(away_val.rstrip('%')) / 100 if '%' in away_val else float(away_val)
            elif 'fouls' in name:
                stats_dict['fouls_home'] = int(home_val)
                stats_dict['fouls_away'] = int(away_val)
            elif 'yellow_cards' in name:
                stats_dict['yellow_cards_home'] = int(home_val)
                stats_dict['yellow_cards_away'] = int(away_val)
            elif 'red_cards' in name:
                stats_dict['red_cards_home'] = int(home_val)
                stats_dict['red_cards_away'] = int(away_val)
            elif 'corner_kicks' in name or 'corners' in name:
                stats_dict['corners_home'] = int(home_val)
                stats_dict['corners_away'] = int(away_val)
            elif 'offsides' in name:
                stats_dict['offsides_home'] = int(home_val)
                stats_dict['offsides_away'] = int(away_val)
            # Aggiungi altri mapping se necessario
    
    # Insert into columns
    insert_column_query = """
    INSERT INTO match_statistics_column (
        match_id, possession_home, possession_away, shots_total_home, shots_total_away,
        shots_on_target_home, shots_on_target_away, passes_total_home, passes_total_away,
        pass_accuracy_home, pass_accuracy_away, fouls_home, fouls_away,
        yellow_cards_home, yellow_cards_away, red_cards_home, red_cards_away,
        corners_home, corners_away, offsides_home, offsides_away
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (match_id) DO UPDATE SET
        possession_home = EXCLUDED.possession_home,
        possession_away = EXCLUDED.possession_away,
        shots_total_home = EXCLUDED.shots_total_home,
        shots_total_away = EXCLUDED.shots_total_away,
        shots_on_target_home = EXCLUDED.shots_on_target_home,
        shots_on_target_away = EXCLUDED.shots_on_target_away,
        passes_total_home = EXCLUDED.passes_total_home,
        passes_total_away = EXCLUDED.passes_total_away,
        pass_accuracy_home = EXCLUDED.pass_accuracy_home,
        pass_accuracy_away = EXCLUDED.pass_accuracy_away,
        fouls_home = EXCLUDED.fouls_home,
        fouls_away = EXCLUDED.fouls_away,
        yellow_cards_home = EXCLUDED.yellow_cards_home,
        yellow_cards_away = EXCLUDED.yellow_cards_away,
        red_cards_home = EXCLUDED.red_cards_home,
        red_cards_away = EXCLUDED.red_cards_away,
        corners_home = EXCLUDED.corners_home,
        corners_away = EXCLUDED.corners_away,
        offsides_home = EXCLUDED.offsides_home,
        offsides_away = EXCLUDED.offsides_away;
    """
    values = (
        match_id,
        stats_dict.get('possession_home'),
        stats_dict.get('possession_away'),
        stats_dict.get('shots_total_home'),
        stats_dict.get('shots_total_away'),
        stats_dict.get('shots_on_target_home'),
        stats_dict.get('shots_on_target_away'),
        stats_dict.get('passes_total_home'),
        stats_dict.get('passes_total_away'),
        stats_dict.get('pass_accuracy_home'),
        stats_dict.get('pass_accuracy_away'),
        stats_dict.get('fouls_home'),
        stats_dict.get('fouls_away'),
        stats_dict.get('yellow_cards_home'),
        stats_dict.get('yellow_cards_away'),
        stats_dict.get('red_cards_home'),
        stats_dict.get('red_cards_away'),
        stats_dict.get('corners_home'),
        stats_dict.get('corners_away'),
        stats_dict.get('offsides_home'),
        stats_dict.get('offsides_away')
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_json_query, (match_id, extras.Json(statistics)))
            cursor.execute(insert_column_query, values)
        conn.commit()
        print(f"Statistiche inserite per match {match_id}.")
    except Exception as e:
        print(f"Errore nell'inserimento delle statistiche per match {match_id}: {e}")

def save_statistics_to_db(match_id, statistics, db_config=None):
    if db_config is None:
        db_config = {
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'password',
            'host': 'localhost',
            'port': '5432'
        }
    conn = create_connection(**db_config)
    if conn:
        create_statistics_table(conn)
        insert_statistics(conn, match_id, statistics)
        conn.close()
    else:
        print("Impossibile connettersi al database per le statistiche.")