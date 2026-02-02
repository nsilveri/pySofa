import time
import logging
from datetime import datetime

import get_matches_per_day
import db_module
from selenium import webdriver

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def print_matches(page_json):
    """Estrazione e stampa dei dati delle partite (per scopi di debug)"""
    for event in page_json['events']:
        home_team = event['homeTeam']['name']
        away_team = event['awayTeam']['name']
        status = event['status']['description']
        tournament = event['tournament']['name']
        
        home_score = event.get('homeScore', {}).get('current', 'N/A')
        away_score = event.get('awayScore', {}).get('current', 'N/A')
        
        # Dati aggiuntivi
        start_timestamp = event.get('startTimestamp', 'N/A')
        home_country = event['homeTeam']['country']['name']
        away_country = event['awayTeam']['country']['name']
        season = event['season']['name']
        event_id = event['id']
        
        print(f"Torneo: {tournament}")
        print(f"Stagione: {season}")
        print(f"{home_team} ({home_country}) {home_score} - {away_score} {away_team} ({away_country})")
        print(f"Status: {status}")
        print(f"ID Evento: {event_id}")
        print(f"Timestamp Inizio: {start_timestamp}")
        print("-" * 50)

def main(date_str='2026-01-18'):
    start_time = time.time()
    logging.info(f"Avvio elaborazione per la data: {date_str}")
    
    # Configurazione Selenium
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(10)

    # Inizializza connessione DB unica
    conn = db_module.create_connection()
    if not conn:
        logging.error("Impossibile stabilire una connessione al database.")
        driver.quit()
        return

    try:
        data = get_matches_per_day.get_matches_data(date_str, driver=driver)
        if not data:
            logging.error("Dati non trovati per la data specificata.")
            return

        # Salva i match base
        db_module.save_matches_to_db(data, conn=conn)
        
        events = data.get('events', [])
        total_events = len(events)
        
        for i, event in enumerate(events):
            match_id = event['id']
            logging.info(f"Processando match {match_id} ({i+1}/{total_events})")
            
            # Grafici
            graphics = get_matches_per_day.get_graphics_per_match(match_id, driver)
            if graphics:
                db_module.save_graphics_to_db(match_id, graphics, conn=conn)
            else:
                logging.warning(f"Nessun grafico trovato per match {match_id}")
            
            # Statistiche
            statistics = get_matches_per_day.get_statistics_per_match(match_id, driver)
            if statistics:
                db_module.save_statistics_to_db(match_id, statistics, conn=conn)
            else:
                logging.warning(f"Nessuna statistica trovata per match {match_id}")
        
        # Popola la tabella statistics_column dopo aver inserito tutti i JSON
        db_module.populate_statistics_column_db(conn=conn)
    
    except Exception as e:
        logging.exception(f"Errore durante l'esecuzione: {e}")
    finally:
        if driver:
            driver.quit()
        if conn:
            conn.close()
            logging.info("Connessione al database chiusa.")
    end_time = time.time()
    logging.info(f"Tempo di esecuzione totale: {end_time - start_time:.2f} secondi")

if __name__ == "__main__":
    # Si potrebbe aggiungere argparse qui per passare la data da riga di comando
    target_date = '2026-01-18'
    main(target_date)
