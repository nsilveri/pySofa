from .config import DB_CONFIG
import psycopg2
from psycopg2 import sql, extras

def create_connection(config=DB_CONFIG):
    try:
        conn = psycopg2.connect(**config)
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
        away_country TEXT,
        home_score_ht INT,
        away_score_ht INT
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
    INSERT INTO matches (id, tournament, season, home_team, away_team, home_score, away_score, status, start_timestamp, home_country, away_country, home_score_ht, away_score_ht)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    event['homeTeam'].get('country', {}).get('name', 'N/A'),
                    event['awayTeam'].get('country', {}).get('name', 'N/A'),
                    event.get('homeScore', {}).get('period1'),
                    event.get('awayScore', {}).get('period1')
                )
                cursor.execute(insert_query, data)
        conn.commit()
        print(f"Inseriti {len(events)} eventi nel database.")
    except Exception as e:
        print(f"Errore nell'inserimento dei dati: {e}")

def save_matches_to_db(events, conn=None):
    should_close = False
    if conn is None:
        conn = create_connection()
        should_close = True
    
    if conn:
        create_table(conn)
        insert_matches(conn, events)
        if should_close:
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
        # print("Tabelle 'match_graphics_json' e 'match_graphics_column' create o già esistenti.")
    except Exception as e:
        print(f"Errore nella creazione delle tabelle graphics: {e}")

def insert_graphics(conn, match_id, graphics):
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

def save_graphics_to_db(match_id, graphics, conn=None):
    should_close = False
    if conn is None:
        conn = create_connection()
        should_close = True
        
    if conn:
        create_graphics_table(conn)
        insert_graphics(conn, match_id, graphics)
        if should_close:
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
        # print("Tabelle 'match_statistics_json' e 'match_statistics_column' create o già esistenti.")
    except Exception as e:
        print(f"Errore nella creazione delle tabelle statistics: {e}")

def insert_statistics(conn, match_id, statistics):
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
        # print(f"Statistiche JSON inserite per match {match_id}.")
    except Exception as e:
        print(f"Errore nell'inserimento delle statistiche JSON per match {match_id}: {e}")

def save_statistics_to_db(match_id, statistics, conn=None):
    should_close = False
    if conn is None:
        conn = create_connection()
        should_close = True
        
    if conn:
        create_statistics_table(conn)
        insert_statistics(conn, match_id, statistics)
        if should_close:
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

def populate_statistics_column_db(conn=None):
    should_close = False
    if conn is None:
        conn = create_connection()
        should_close = True
        
    if conn:
        populate_statistics_column(conn)
        if should_close:
            conn.close()
    else:
        print("Impossibile connettersi al database per popolare le statistiche colonne.")

def create_incidents_table(conn):
    """Crea le tabelle per gli incidenti (JSON e Colonne)."""
    create_json_query = """
    CREATE TABLE IF NOT EXISTS match_incidents_json (
        match_id BIGINT PRIMARY KEY,
        incidents JSONB
    );
    """
    create_column_query = """
    CREATE TABLE IF NOT EXISTS match_incidents_column (
        match_id BIGINT,
        time INT,
        added_time INT,
        incident_type TEXT,
        team_side TEXT,
        player_name TEXT,
        home_score INT,
        away_score INT
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_json_query)
            cursor.execute(create_column_query)
        conn.commit()
    except Exception as e:
        print(f"Errore nella creazione delle tabelle incidents: {e}")

def insert_incidents(conn, match_id, incidents_data):
    """Inserisce gli incidenti sia in formato JSON che in colonne."""
    # 1. Inserimento JSON
    insert_json_query = """
    INSERT INTO match_incidents_json (match_id, incidents)
    VALUES (%s, %s)
    ON CONFLICT (match_id) DO UPDATE SET incidents = EXCLUDED.incidents;
    """
    
    # 2. Preparazione dati per Colonne
    incidents_list = incidents_data.get('incidents', [])
    column_data = []
    for inc in incidents_list:
        column_data.append((
            match_id,
            inc.get('time'),
            inc.get('addedTime', 0),
            inc.get('incidentType', inc.get('type')),
            inc.get('teamSide'),
            inc.get('player', {}).get('name'),
            inc.get('homeScore'),
            inc.get('awayScore')
        ))
    
    insert_column_query = """
    INSERT INTO match_incidents_column (
        match_id, time, added_time, incident_type, team_side, player_name, home_score, away_score
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    try:
        with conn.cursor() as cursor:
            # Salviamo il JSON
            cursor.execute(insert_json_query, (match_id, extras.Json(incidents_data)))
            
            # Puliamo i vecchi record in colonna per questo match ed inseriamo i nuovi
            cursor.execute("DELETE FROM match_incidents_column WHERE match_id = %s", (match_id,))
            if column_data:
                extras.execute_batch(cursor, insert_column_query, column_data)
                
        conn.commit()
        print(f"Incidenti (JSON + Colonne) inseriti per match {match_id}.")
    except Exception as e:
        print(f"Errore nell'inserimento degli incidenti per match {match_id}: {e}")

def save_incidents_to_db(match_id, incidents, conn=None):
    """Salva gli incidenti nel database, gestendo la connessione."""
    should_close = False
    if conn is None:
        conn = create_connection()
        should_close = True
        
    if conn:
        create_incidents_table(conn)
        insert_incidents(conn, match_id, incidents)
        if should_close:
            conn.close()
    else:
        print("Impossibile connettersi al database per gli incidenti.")
