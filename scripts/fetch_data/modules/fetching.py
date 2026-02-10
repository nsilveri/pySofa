import logging
import time
from selenium import webdriver
from . import db_module
from . import get_matches_per_day

def setup_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(15)
    return driver

def process_date(date_str, headless_mode=True):
    driver = None
    conn = None
    
    try:
        logging.debug(f"Inzio elaborazione per data: {date_str}")
        
        # 1. Setup Driver
        driver = setup_driver(headless_mode)
        
        # 2. Connessione DB
        conn = db_module.create_connection()
        if not conn:
            logging.error("Impossibile connettersi al DB.")
            return

        # 3. Download Lista Match
        logging.debug("Scaricamento lista partite...")
        data = get_matches_per_day.get_matches_data(date_str, driver=driver)
        
        if not data or 'events' not in data:
            logging.warning("Nessun dato trovato o errore nel download.")
            return

        events = data.get('events', [])
        total = len(events)
        logging.debug(f"Trovati {total} eventi.")

        # 4. Salvataggio Match Base
        db_module.save_matches_to_db(events, conn=conn)
        
        from tqdm.auto import tqdm
        
        # 5. Loop dettagli (Statistiche, Grafici e Incidenti)
        new_matches_processed = 0
        pbar = tqdm(events, desc=f"Partite {date_str}", unit="match", leave=False)
        for i, event in enumerate(pbar):
            match_id = event['id']
            home_team = event['homeTeam']['name']
            away_team = event['awayTeam']['name']
            
            # --- CONTROLLO ESISTENZA ---
            if db_module.check_match_exists(match_id, conn):
                continue
            
            pbar.set_description(f"Data: {date_str} | {home_team} vs {away_team}")
            
            # Scarica e salva Grafici
            graphics = get_matches_per_day.get_graphics_per_match(match_id, driver)
            if graphics:
                db_module.save_graphics_to_db(match_id, graphics, conn=conn)
            
            # Scarica e salva Statistiche
            statistics = get_matches_per_day.get_statistics_per_match(match_id, driver)
            if statistics:
                db_module.save_statistics_to_db(match_id, statistics, conn=conn)
            
            # Scarica e salva Incidenti (Goal, cartellini, ecc.)
            incidents = get_matches_per_day.get_incidents_per_match(match_id, driver)
            if incidents:
                db_module.save_incidents_to_db(match_id, incidents, conn=conn)
            
            new_matches_processed += 1
            # Piccolo sleep per cortesia
            time.sleep(0.5)
        pbar.close()
        
        # 6. Aggiorna colonne statistiche (SOLO SE ABBIAMO NUOVI DATI)
        if new_matches_processed > 0:
            logging.info(f"Rielaborazione colonne statistiche per {new_matches_processed} nuovi match...")
            db_module.populate_statistics_column_db(conn=conn)
        logging.info(f"Data {date_str} completata.")
        
    except Exception as e:
        logging.error(f"Errore critico: {e}")
    finally:
        if driver:
            driver.quit()
        if conn:
            conn.close()
