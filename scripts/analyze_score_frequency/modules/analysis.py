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

def get_stats_dataset(stats_list, period='ALL'):
    """
    Recupera un dataset pronto per la regressione lineare o LSTM.
    Restituisce un dataframe dove ogni riga è la performance di UNA squadra in UN match.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    # Rimuoviamo eventuali colonne di metadata dalla lista delle statistiche per evitare duplicati
    clean_stats = [s for s in stats_list if s not in ['match_id', 'team', 'opponent', 'is_home', 'date', 'start_timestamp']]
    
    placeholders = ', '.join(['%s'] * len(clean_stats))
    query = f"""
    SELECT m.id as match_id, m.home_team, m.away_team, m.start_timestamp, msc.name, msc.homevalue, msc.awayvalue
    FROM matches m
    JOIN match_statistics_column msc ON m.id = msc.match_id
    WHERE msc.period = %s AND msc.name IN ({placeholders})
    """
    params = [period] + clean_stats
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    
    if df.empty: return pd.DataFrame()
    
    # Pulizia valori (rimozione testi tra parentesi come "5(2)")
    df['homevalue'] = df['homevalue'].astype(str).str.split('(').str[0].str.strip().apply(pd.to_numeric, errors='coerce')
    df['awayvalue'] = df['awayvalue'].astype(str).str.split('(').str[0].str.strip().apply(pd.to_numeric, errors='coerce')
    
    # Pivot per avere le statistiche come colonne
    pivoted = df.pivot_table(index=['match_id', 'home_team', 'away_team', 'start_timestamp'], columns='name', values=['homevalue', 'awayvalue'])
    
    # Creazione dataset "Flat" (due righe per match: una per home, una per away)
    home_data = pivoted.xs('homevalue', axis=1, level=0).reset_index()
    home_data['team'] = home_data['home_team']
    home_data['opponent'] = home_data['away_team']
    home_data['date'] = pd.to_datetime(home_data['start_timestamp'], unit='s')
    home_data['is_home'] = 1
    
    away_data = pivoted.xs('awayvalue', axis=1, level=0).reset_index()
    away_data['team'] = away_data['away_team']
    away_data['opponent'] = away_data['home_team']
    away_data['date'] = pd.to_datetime(away_data['start_timestamp'], unit='s')
    away_data['is_home'] = 0
    
    # Uniamo i due set di dati
    cols_to_keep = clean_stats + ['match_id', 'team', 'opponent', 'is_home', 'date']
    final_df = pd.concat([
        home_data[cols_to_keep],
        away_data[cols_to_keep]
    ], ignore_index=True)
    
    return final_df

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

def get_matches_by_date(date_str):
    """
    Recupera i match di una determinata data (YYYY-MM-DD).
    """
    conn = psycopg2.connect(**DB_CONFIG)
    # Convertiamo la stringa data in range di timestamp
    start_ts = int(pd.to_datetime(date_str).timestamp())
    end_ts = start_ts + 86400  # + 24 ore
    
    query = """
    SELECT id, home_team, away_team, home_score, away_score, 
           home_score_ht, away_score_ht, tournament, start_timestamp
    FROM matches
    WHERE start_timestamp >= %s AND start_timestamp < %s
    """
    df_res = pd.read_sql(query, conn, params=[start_ts, end_ts])
    conn.close()
    return df_res

def get_first_match_with_xg():
    """
    Ritorna il primo match (il più vecchio) che ha dati xG disponibili.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
    SELECT m.*
    FROM matches m
    JOIN match_statistics_column msc ON m.id = msc.match_id
    WHERE msc.name ILIKE 'Expected goals'
    ORDER BY m.start_timestamp ASC
    LIMIT 1
    """
    df_res = pd.read_sql(query, conn)
    conn.close()
    
    if not df_res.empty:
        df_res['date_readable'] = pd.to_datetime(df_res['start_timestamp'], unit='s').dt.strftime('%Y-%m-%d %H:%M')
        
    return df_res

def get_match_incidents(match_id):
    """Recupera tutti gli incidenti per un determinato match."""
    conn = psycopg2.connect(**DB_CONFIG)
    query = "SELECT * FROM match_incidents_column WHERE match_id = %s ORDER BY time ASC, added_time ASC"
    df = pd.read_sql(query, conn, params=[int(match_id)])
    conn.close()
    return df

def get_match_statistics(match_id):
    """Recupera tutte le statistiche per un determinato match."""
    conn = psycopg2.connect(**DB_CONFIG)
    query = "SELECT * FROM match_statistics_column WHERE match_id = %s"
    df = pd.read_sql(query, conn, params=[int(match_id)])
    conn.close()
    return df

def get_match_by_team_and_date(team_name, date_str):
    """
    Trova un match basato sul nome della squadra (anche parziale) e la data (YYYY-MM-DD).
    """
    conn = psycopg2.connect(**DB_CONFIG)
    start_ts = int(pd.to_datetime(date_str).timestamp())
    end_ts = start_ts + 86400
    
    query = """
    SELECT * FROM matches 
    WHERE (home_team ILIKE %s OR away_team ILIKE %s)
      AND start_timestamp >= %s AND start_timestamp < %s
    """
    search_term = f"%{team_name}%"
    df = pd.read_sql(query, conn, params=[search_term, search_term, start_ts, end_ts])
    conn.close()
    
    if not df.empty:
        df['date_readable'] = pd.to_datetime(df['start_timestamp'], unit='s').dt.strftime('%Y-%m-%d %H:%M')
        
    return df

def get_len_table(table_name):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = f"SELECT COUNT(*) FROM {table_name}"
    cursor.execute(query)
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return int(count)
