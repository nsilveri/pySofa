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
    CREATE TABLE IF NOT EXISTS matches_json (
        id BIGINT PRIMARY KEY,
        match_data JSONB
    );
    """)
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
        conn.commit()
        print("Tabella 'matches_json' creata o già esistente.")
    except Exception as e:
        print(f"Errore nella creazione della tabella: {e}")

def insert_matches(conn, events):
    insert_query = """
    INSERT INTO matches_json (id, match_data)
    VALUES (%s, %s)
    ON CONFLICT (id) DO UPDATE SET match_data = EXCLUDED.match_data;
    """
    try:
        with conn.cursor() as cursor:
            for event in events:
                cursor.execute(insert_query, (event['id'], extras.Json(event)))
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
    create_table_query = """
    CREATE TABLE IF NOT EXISTS match_graphics_json (
        match_id BIGINT PRIMARY KEY,
        graphics JSONB
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
        conn.commit()
        print("Tabella 'match_graphics_json' creata o già esistente.")
    except Exception as e:
        print(f"Errore nella creazione della tabella graphics: {e}")

def insert_graphics(conn, match_id, graphics):
    insert_query = """
    INSERT INTO match_graphics_json (match_id, graphics)
    VALUES (%s, %s)
    ON CONFLICT (match_id) DO UPDATE SET graphics = EXCLUDED.graphics;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_query, (match_id, extras.Json(graphics)))
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
    create_table_query = """
    CREATE TABLE IF NOT EXISTS match_statistics_json (
        match_id BIGINT PRIMARY KEY,
        statistics JSONB
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
        conn.commit()
        print("Tabella 'match_statistics_json' creata o già esistente.")
    except Exception as e:
        print(f"Errore nella creazione della tabella statistics: {e}")

def insert_statistics(conn, match_id, statistics):
    insert_query = """
    INSERT INTO match_statistics_json (match_id, statistics)
    VALUES (%s, %s)
    ON CONFLICT (match_id) DO UPDATE SET statistics = EXCLUDED.statistics;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_query, (match_id, extras.Json(statistics)))
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