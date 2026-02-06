import pandas as pd
import psycopg2
from .config import DB_CONFIG

def get_matches():
    """Recupera i match dal database inclusi i risultati parziali."""
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
    SELECT 
        id, home_team, away_team, 
        home_score, away_score, 
        home_score_ht, away_score_ht,
        tournament, start_timestamp 
    FROM matches
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_stats_by_period(stats_list=['Expected goals', 'Ball possession'], period='1ST'):
    """Recupera statistiche specifiche per un determinato periodo dalla tabella match_statistics_column."""
    conn = psycopg2.connect(**DB_CONFIG)
    placeholders = ', '.join(['%s'] * len(stats_list))
    query = f"""
    SELECT match_id, name, homeValue, awayValue
    FROM match_statistics_column
    WHERE period = %s AND name IN ({placeholders})
    """
    params = [period] + stats_list
    df_s = pd.read_sql(query, conn, params=params)
    conn.close()
    
    if df_s.empty: return pd.DataFrame()

    # Rimuoviamo eventuali duplicati per lo stesso match/statistica (data integrity)
    df_s = df_s.drop_duplicates(subset=['match_id', 'name'], keep='last')

    # Riorganizziamo il dataframe per avere le statistiche come colonne
    df_pivot = df_s.pivot(index='match_id', columns='name', values=['homevalue', 'awayvalue'])
    df_pivot.columns = [f"{col[1].lower().replace(' ', '_')}_{col[0]}" for col in df_pivot.columns]
    return df_pivot.reset_index()

def get_matches_by_partial_score(target_h, target_a, minute):
    """
    Trova i match che avevano un determinato punteggio (target_h - target_a) al minuto indicato.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    
    if target_h == 0 and target_a == 0:
        # Caso 0-0: Match dove non ci sono goal segnati entro quel minuto
        query = """
        SELECT m.id, m.home_team, m.away_team, 0 as score_h, 0 as score_a, m.home_score as final_h, m.away_score as final_a
        FROM matches m
        WHERE NOT EXISTS (
            SELECT 1 FROM match_incidents_column mic 
            WHERE mic.match_id = m.id 
              AND mic.incident_type ILIKE '%%goal%%'
              AND mic.time <= %s
        )
        """
        df_res = pd.read_sql(query, conn, params=[minute])
    else:
        # Caso con goal: Cerchiamo l'ultimo goal avvenuto entro il minuto
        query = """
        WITH LastGoal AS (
            SELECT 
                match_id, 
                home_score, 
                away_score,
                ROW_NUMBER() OVER(PARTITION BY match_id ORDER BY time DESC, added_time DESC) as rn
            FROM match_incidents_column
            WHERE incident_type ILIKE '%%goal%%'
              AND time <= %s
              AND home_score IS NOT NULL AND away_score IS NOT NULL
        )
        SELECT m.id, m.home_team, m.away_team, lg.home_score as score_h, lg.away_score as score_a, m.home_score as final_h, m.away_score as final_a
        FROM matches m
        JOIN LastGoal lg ON m.id = lg.match_id
        WHERE lg.rn = 1 AND lg.home_score = %s AND lg.away_score = %s
        """
        df_res = pd.read_sql(query, conn, params=[minute, target_h, target_a])
        
    conn.close()
    
    # Assicuriamoci che i punteggi siano numerici
    for col in ['score_h', 'score_a', 'final_h', 'final_a']:
        if col in df_res.columns:
            df_res[col] = pd.to_numeric(df_res[col], errors='coerce')
            
    return df_res

def get_matches_by_ht_score(target_h, target_a):
    """
    Trova i match che hanno un determinato punteggio al primo tempo usando le colonne home_score_ht e away_score_ht.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
    SELECT id, home_team, away_team, home_score_ht, away_score_ht, home_score as final_h, away_score as final_a
    FROM matches
    WHERE home_score_ht = %s AND away_score_ht = %s
    """
    df_res = pd.read_sql(query, conn, params=[target_h, target_a])
    conn.close()
    
    # Assicuriamoci che i punteggi siano numerici per evitare errori nei calcoli
    for col in ['home_score_ht', 'away_score_ht', 'final_h', 'final_a']:
        if col in df_res.columns:
            df_res[col] = pd.to_numeric(df_res[col], errors='coerce')
            
    return df_res

def get_len_table(table_name):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = f"SELECT COUNT(*) FROM {table_name}"
    cursor.execute(query)
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return int(count)
