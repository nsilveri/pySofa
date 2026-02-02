import pandas as pd
import numpy as np
import json
import logging
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import db_module

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data_from_db():
    conn = db_module.create_connection()
    if not conn:
        logging.error("Connessione al database fallita.")
        return None, None

    try:
        # 1. Recupero statistiche (solo periodo "ALL")
        stats_query = """
            SELECT match_id, key, homevalue, awayvalue 
            FROM match_statistics_column 
            WHERE period = 'ALL';
        """
        df_stats_raw = pd.read_sql(stats_query, conn)

        # 2. Recupero grafici per feature momentum
        graphics_query = "SELECT match_id, graphics FROM match_graphics_json;"
        df_graphics_raw = pd.read_sql(graphics_query, conn)

        # 3. Recupero info base match (per nomi squadre e risultati)
        matches_query = "SELECT id, home_team, away_team, home_score, away_score FROM matches;"
        df_matches = pd.read_sql(matches_query, conn)

        return df_stats_raw, df_graphics_raw, df_matches
    finally:
        conn.close()

def process_features(df_stats, df_graphics):
    logging.info("Elaborazione features per il clustering...")
    
    # Rimuovi duplicati: alcune statistiche (es. "Total Shots") compaiono in più gruppi (Match Overview, Shots, ecc.)
    # Teniamo solo la prima occorrenza per ogni match_id e key
    df_stats = df_stats.drop_duplicates(subset=['match_id', 'key'])
    
    # Pivot delle statistiche: vogliamo una riga per match_id
    # Prepariamo i dati delle statistiche
    df_pivot_home = df_stats.pivot(index='match_id', columns='key', values='homevalue').add_prefix('home_')
    df_pivot_away = df_stats.pivot(index='match_id', columns='key', values='awayvalue').add_prefix('away_')
    df_features = pd.concat([df_pivot_home, df_pivot_away], axis=1)

    # Elaborazione Momentum dai grafici JSON
    momentum_features = []
    for index, row in df_graphics.iterrows():
        m_id = row['match_id']
        try:
            data = row['graphics'] # Già caricato come dict/list da pandas se JSONB, o stringa
            if isinstance(data, str):
                data = json.loads(data)
            
            points = data.get('graphPoints', [])
            if points:
                values = [p.get('value', 0) for p in points]
                momentum_features.append({
                    'match_id': m_id,
                    'momentum_avg': np.mean(values),
                    'momentum_abs_avg': np.mean(np.abs(values)),
                    'momentum_std': np.std(values),
                    'momentum_max': np.max(values),
                    'momentum_min': np.min(values)
                })
        except Exception as e:
            logging.warning(f"Errore nel processare grafica per match {m_id}: {e}")

    if momentum_features:
        df_momentum = pd.DataFrame(momentum_features).set_index('match_id')
        df_features = df_features.join(df_momentum, how='left')

    # Pulizia: riempiamo i valori mancanti con la media (o 0)
    df_features = df_features.fillna(0)
    
    return df_features

def run_clustering(df_features, n_clusters=4):
    logging.info(f"Esecuzione KMeans con {n_clusters} cluster...")
    
    # Scaling dei dati (fondamentale per KMeans)
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(df_features)
    
    # Algoritmo KMeans
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df_features['cluster'] = kmeans.fit_predict(scaled_features)
    
    return df_features, kmeans

def main():
    # 1. Caricamento dati
    df_stats, df_graphics, df_matches = fetch_data_from_db()
    if df_stats is None or df_stats.empty:
        logging.error("Nessun dato trovato per il clustering.")
        return

    # 2. Trasformazione dati
    df_final = process_features(df_stats, df_graphics)
    
    # 3. Clustering
    n_clusters = 4
    df_clustered, model = run_clustering(df_final, n_clusters=n_clusters)
    
    # 4. Merge con informazioni leggibili (nomi squadre)
    df_results = df_matches.set_index('id').join(df_clustered[['cluster']], how='inner')
    
    # 5. Output risultati
    print("\n" + "="*50)
    print("RISULTATI CLUSTERING MATCH")
    print("="*50)
    
    for cluster_id in range(n_clusters):
        print(f"\nCLUSTER {cluster_id}:")
        members = df_results[df_results['cluster'] == cluster_id]
        for idx, row in members.head(10).iterrows():
            print(f" - {row['home_team']} {row['home_score']} - {row['away_score']} {row['away_team']}")
        if len(members) > 10:
            print(f"   ... e altri {len(members) - 10} match")

    # Analisi dei centroidi per capire cosa rappresentano i cluster (opzionale)
    print("\nInformazione: I cluster dividono i match in base a statistiche e inerzia (momentum).")
    print("Puoi regolare 'n_clusters' nel codice per cambiare il numero di gruppi.")

if __name__ == "__main__":
    main()
