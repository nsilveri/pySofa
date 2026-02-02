import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os

# Aggiungiamo la root del progetto al path per importare db_module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import db_module
import logging

# =================================================================
# CONFIGURAZIONE UTENTE
# =================================================================
# Se True, mostra la distribuzione di TUTTI i risultati frequenti.
# Se False, monitora solo la % del TARGET_SCORE scelto.
ANALYZE_ALL = True

# Imposta il risultato specifico (es: "2-1", "1-1") - Usato se ANALYZE_ALL = False
TARGET_SCORE = "2-4" 

# Imposta la dimensione degli intervalli (es: 2 per fare 0-2, 2-4, 4-6...)
BIN_SIZE = 2

# Quanti dei risultati più comuni mostrare nel grafico globale
TOP_N_SCORES = 20
# =================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
sns.set_theme(style="whitegrid")

def fetch_match_results():
    conn = db_module.create_connection()
    if not conn:
        return None
    
    try:
        # Recuperiamo i gol e l'ID del match dalla tabella matches
        #Nota: home_score e away_score sono TEXT nel DB, li castiamo a INT per sommarli
        query = """
            SELECT 
                CAST(NULLIF(home_score, 'N/A') AS INTEGER) as home, 
                CAST(NULLIF(away_score, 'N/A') AS INTEGER) as away,
                home_score || '-' || away_score as score_string
            FROM matches
            WHERE home_score IS NOT NULL AND away_score IS NOT NULL 
              AND home_score != 'N/A' AND away_score != 'N/A';
        """
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

def main():
    df = fetch_match_results()
    if df is None or df.empty:
        logging.error("Nessun dato trovato per l'analisi.")
        return

    total_matches = len(df)
    plt.figure(figsize=(12, 8))

    if ANALYZE_ALL:
        logging.info(f"Analisi della frequenza globale di tutti i risultati (Top {TOP_N_SCORES})...")
        
        # Calcoliamo la frequenza di ogni risultato
        score_counts = df['score_string'].value_counts()
        score_percentages = (score_counts / total_matches) * 100
        
        # Prendiamo i top N
        top_analysis = score_percentages.head(TOP_N_SCORES).reset_index()
        top_analysis.columns = ['score_string', 'percentage']

        # Grafico a barre orizzontali per leggibilità
        ax = sns.barplot(x='percentage', y='score_string', data=top_analysis, palette='magma', hue='score_string', legend=False)
        
        for p in ax.patches:
            width = p.get_width()
            ax.annotate(f'{width:.1f}%', (width, p.get_y() + p.get_height() / 2.), 
                       ha='left', va='center', xytext=(5, 0), textcoords='offset points')

        plt.title(f'Top {TOP_N_SCORES} Risultati più Frequenti (Totale Match: {total_matches})', fontsize=15)
        plt.xlabel('% di Occorrenza')
        plt.ylabel('Punteggio Finale')
        plt.xlim(0, top_analysis['percentage'].max() + 2)
        
        filename = "overall_score_frequency.png"
    else:
        logging.info(f"Analisi della frequenza globale per il risultato {TARGET_SCORE}...")
        
        target_count = len(df[df['score_string'] == TARGET_SCORE])
        percentage = (target_count / total_matches) * 100
        
        # Grafico semplice a singola barra
        plt.bar([TARGET_SCORE], [percentage], color='skyblue')
        plt.text(0, percentage + 0.1, f'{percentage:.2f}%', ha='center', fontsize=12)
        
        plt.title(f'Frequenza del Risultato {TARGET_SCORE}', fontsize=15)
        plt.ylabel('% di Occorrenza')
        plt.ylim(0, max(percentage + 1, 5))
        
        filename = f"overall_frequency_{TARGET_SCORE.replace('-', '_')}.png"

    plt.tight_layout()
    plt.savefig(filename)
    logging.info(f"Grafico salvato come: {filename}")
    plt.show()

if __name__ == "__main__":
    main()
