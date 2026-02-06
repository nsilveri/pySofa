import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'analyze_score_frequency', 'modules')))
import pandas as pd
import numpy as np
import json
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import db_module

# Configurazione logging ed estetica grafici
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
sns.set_theme(style="whitegrid")

def fetch_data():
    conn = db_module.create_connection()
    if not conn: return None, None, None
    
    try:
        # Statistiche (ALL)
        df_stats = pd.read_sql('SELECT match_id, key, homevalue, awayvalue FROM match_statistics_column WHERE period = \'ALL\'', conn)
        # Grafici (Momentum)
        df_graphics = pd.read_sql('SELECT match_id, graphics FROM match_graphics_json', conn)
        # Info Match
        df_matches = pd.read_sql('SELECT id, home_team, away_team FROM matches', conn)
        return df_stats, df_graphics, df_matches
    finally:
        conn.close()

def extract_momentum_series(df_graphics):
    """Estrae la serie temporale del momentum (valori al minuto)"""
    series_data = []
    for _, row in df_graphics.iterrows():
        try:
            data = row['graphics']
            if isinstance(data, str): data = json.loads(data)
            points = data.get('graphPoints', [])
            # Creiamo un array di 90 minuti (riempiendo i buchi)
            curve = np.zeros(90)
            for p in points:
                m = int(float(p.get('minute', 0)))
                if 1 <= m <= 90:
                    curve[m-1] = p.get('value', 0)
            series_data.append({'match_id': row['match_id'], 'curve': curve})
        except: continue
    return pd.DataFrame(series_data)

def main():
    df_stats_raw, df_graphics_raw, df_matches = fetch_data()
    if df_stats_raw is None: return

    # 1. Preparazione Features (Statistiche + Momentum Summary)
    df_stats = df_stats_raw.drop_duplicates(subset=['match_id', 'key'])
    pivot_home = df_stats.pivot(index='match_id', columns='key', values='homevalue').add_prefix('h_')
    pivot_away = df_stats.pivot(index='match_id', columns='key', values='awayvalue').add_prefix('a_')
    df_feat = pd.concat([pivot_home, pivot_away], axis=1).fillna(0)
    
    # 2. Preparazione Curves (Trend temporale)
    df_curves = extract_momentum_series(df_graphics_raw)
    if df_curves.empty:
        logging.error("Nessun dato momentum trovato.")
        return
    
    # Uniamo solo i match che hanno sia stats che curves
    df_combined = df_feat.join(df_curves.set_index('match_id'), how='inner')
    
    # 3. Clustering su features statistiche
    X = df_combined.drop(columns=['curve'])
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    n_clusters = 4
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df_combined['cluster'] = kmeans.fit_predict(X_scaled)
    
    logging.info("Generazione grafici...")
    
    # --- GRAFICO 1: ANDAMENTO MOMENTUM MEDIO PER CLUSTER ---
    plt.figure(figsize=(12, 6))
    time_bins = np.arange(1, 91)
    
    for c in range(n_clusters):
        cluster_curves = np.stack(df_combined[df_combined['cluster'] == c]['curve'].values)
        avg_curve = np.mean(cluster_curves, axis=0)
        std_curve = np.std(cluster_curves, axis=0)
        
        plt.plot(time_bins, avg_curve, label=f'Cluster {c}', linewidth=2)
        plt.fill_between(time_bins, avg_curve - std_curve*0.2, avg_curve + std_curve*0.2, alpha=0.1)

    plt.title('Andamento Momentum Medio (Pressione) per Cluster', fontsize=15)
    plt.xlabel('Minuto di gioco')
    plt.ylabel('Valore Momentum (Positivo = Casa, Negativo = Trasferta)')
    plt.axhline(0, color='black', linestyle='--', alpha=0.5)
    plt.legend()
    plt.savefig('cluster_momentum_trends.png')
    logging.info("Creato: cluster_momentum_trends.png")

    # --- GRAFICO 2: PCA - DISTRIBUZIONE DEI MATCH ---
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)
    
    plt.figure(figsize=(10, 7))
    scatter = plt.scatter(X_pca[:, 0], X_pca[:, 1], c=df_combined['cluster'], cmap='viridis', alpha=0.7)
    plt.colorbar(scatter, label='Cluster ID')
    plt.title('Mappa dei Match (PCA)', fontsize=15)
    plt.xlabel('Componente Principale 1')
    plt.ylabel('Componente Principale 2')
    plt.savefig('cluster_map_pca.png')
    logging.info("Creato: cluster_map_pca.png")

    # --- GRAFICO 3: CONFRONTO STATISTICHE CHIAVE ---
    # Selezioniamo alcune stats chiave se esistono
    key_cols = [c for c in df_combined.columns if any(k in c for k in ['ballPossession', 'totalShots', 'expectedGoals'])]
    if key_cols:
        stats_summary = df_combined.groupby('cluster')[key_cols].mean().T
        stats_summary.plot(kind='bar', figsize=(12, 6))
        plt.title('Confronto Medie Statistiche per Cluster', fontsize=15)
        plt.ylabel('Valore Medio')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('cluster_stats_comparison.png')
        logging.info("Creato: cluster_stats_comparison.png")

    plt.show()

if __name__ == "__main__":
    main()
