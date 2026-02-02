import sys
sys.path.append('lib')

# Import psycopg2 only when needed
def _import_psycopg2():
    try:
        import psycopg2
        from psycopg2 import sql
        return psycopg2, sql
    except ImportError as e:
        print(f"Errore nell'import di psycopg2: {e}")
        return None, None

psycopg2, sql = _import_psycopg2()

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
    create_table_query = """
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
    """
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

if __name__ == "__main__":
    conn = create_connection()
    if conn:
        print("Connessione riuscita!")
        create_table(conn)
        conn.close()
    else:
        print("Connessione fallita.")