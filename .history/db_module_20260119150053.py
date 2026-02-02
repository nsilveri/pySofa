import psycopg2
from psycopg2 import sql, extras

def create_connection(dbname='football_db', user='postgres', password='password', host='localhost', port='5432'):
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
            'dbname': 'football_db',
            'user': 'postgres',
            'password': 'postgres',
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

def match_exists(conn, match_id):
    query = "SELECT COUNT(*) FROM matches WHERE id = %s"
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (match_id,))
            count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        print(f"Errore nel controllo esistenza match: {e}")
        return False

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
    #print(f"Graphics data for match {match_id}: {graphics}")  # Debug
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
    #print(f"Possession values extracted: {possession_values}")  # Debug
    
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
            'dbname': 'football_db',
            'user': 'postgres',
            'password': 'postgres',
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
    # Column table - normalized statistics
    create_column_query = """
    CREATE TABLE IF NOT EXISTS match_statistics_column (
        match_id BIGINT,
        period TEXT,
        groupName TEXT,
        name TEXT,
        home TEXT,
        away TEXT,
        compareCode INT,
        statisticsType TEXT,
        valueType TEXT,
        homeValue FLOAT,
        awayValue FLOAT,
        renderType INT,
        key TEXT
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
    #print(f"Statistics data for match {match_id}: {statistics}")  # Debug
    # Insert into JSON
    insert_json_query = """
    INSERT INTO match_statistics_json (match_id, statistics)
    VALUES (%s, %s)
    ON CONFLICT (match_id) DO UPDATE SET statistics = EXCLUDED.statistics;
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_json_query, (match_id, extras.Json(statistics)))
        conn.commit()
        print(f"Statistiche JSON inserite per match {match_id}.")
    except Exception as e:
        print(f"Errore nell'inserimento delle statistiche JSON per match {match_id}: {e}")

def save_statistics_to_db(match_id, statistics, db_config=None):
    if db_config is None:
        db_config = {
            'dbname': 'football_db',
            'user': 'postgres',
            'password': 'postgres',
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

def populate_statistics_column(conn):
    # Drop and recreate the column table
    drop_column_query = "DROP TABLE IF EXISTS match_statistics_column;"
    create_column_query = """
    CREATE TABLE match_statistics_column (
        match_id BIGINT,
        period TEXT,
        groupName TEXT,
        name TEXT,
        home TEXT,
        away TEXT,
        compareCode INT,
        statisticsType TEXT,
        valueType TEXT,
        homeValue FLOAT,
        awayValue FLOAT,
        renderType INT,
        key TEXT
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(drop_column_query)
            cursor.execute(create_column_query)
            
            # Select all JSON statistics
            select_all_json_query = "SELECT match_id, statistics FROM match_statistics_json;"
            cursor.execute(select_all_json_query)
            rows = cursor.fetchall()
            
            # Insert into columns for each match
            insert_column_query = """
            INSERT INTO match_statistics_column (
                match_id, period, groupName, name, home, away, compareCode, statisticsType, valueType, homeValue, awayValue, renderType, key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            for match_id, stored_statistics in rows:
                statistics_list = stored_statistics.get('statistics', [])
                for period_data in statistics_list:
                    period = period_data.get('period')
                    groups = period_data.get('groups', [])
                    for group in groups:
                        groupName = group.get('groupName')
                        statisticsItems = group.get('statisticsItems', [])
                        for stat in statisticsItems:
                            name = stat.get('name')
                            home = stat.get('home')
                            away = stat.get('away')
                            compareCode = stat.get('compareCode')
                            statisticsType = stat.get('statisticsType')
                            valueType = stat.get('valueType')
                            homeValue = stat.get('homeValue')
                            awayValue = stat.get('awayValue')
                            renderType = stat.get('renderType')
                            key = stat.get('key')
                            cursor.execute(insert_column_query, (
                                match_id, period, groupName, name, home, away, compareCode, statisticsType, valueType, homeValue, awayValue, renderType, key
                            ))
        conn.commit()
        print("Tabella match_statistics_column popolata con successo.")
    except Exception as e:
        print(f"Errore nel popolamento della tabella match_statistics_column: {e}")

def populate_statistics_column_db(db_config=None):
    if db_config is None:
        db_config = {
            'dbname': 'football_db',
            'user': 'postgres',
            'password': 'postgres',
            'host': 'localhost',
            'port': '5432'
        }
    conn = create_connection(**db_config)
    if conn:
        populate_statistics_column(conn)
        conn.close()
    else:
        print("Impossibile connettersi al database per popolare le statistiche colonne.")