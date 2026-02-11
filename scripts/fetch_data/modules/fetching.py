import logging
import time
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from . import db_module
from . import get_matches_per_day

MAX_RETRIES = 3
RETRY_WAIT = 5  # secondi di attesa tra i retry

def setup_driver(headless=True):
    options = webdriver.ChromeOptions()
    if headless:
        # Usiamo il flag --headless=new che è più stabile
        options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-extensions')
    options.add_argument('--window-size=1920,1080')
    
    # Aggiungo un User-Agent normale per provare a mitigare il 403
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(45)
    return driver

def _restart_driver(driver, headless_mode):
    """Chiude il driver corrente (se esiste) e ne crea uno nuovo."""
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
    logging.warning("Ricreazione del driver Chrome...")
    time.sleep(RETRY_WAIT)
    return setup_driver(headless_mode)

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
            return False

        # 3. Download Lista Match (con retry)
        logging.debug(f"Scaricamento lista partite per {date_str}...")
        data = None
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                data = get_matches_per_day.get_matches_data(date_str, driver=driver)
                if data:
                    break
                else:
                    logging.warning(f"[{date_str}] Tentativo {attempt+1}/{MAX_RETRIES}: get_matches_data ha ritornato None (nessun JSON estratto)")
                    if attempt < MAX_RETRIES - 1:
                        driver = _restart_driver(driver, headless_mode)
            except Exception as e:
                last_error = e
                logging.warning(f"[{date_str}] Tentativo {attempt+1}/{MAX_RETRIES} ECCEZIONE: {type(e).__name__}: {e}")
                if attempt < MAX_RETRIES - 1:
                    driver = _restart_driver(driver, headless_mode)
        
        if not data:
            logging.error(f"[{date_str}] FALLITO: impossibile scaricare la lista match dopo {MAX_RETRIES} tentativi. Ultimo errore: {last_error}")
            return False
        
        if 'events' not in data:
            error_detail = data.get('error', data)
            logging.warning(f"[{date_str}] JSON ricevuto ma senza 'events'. Contenuto errore: {error_detail}")
            return False

        events = data.get('events', [])
        total = len(events)
        logging.info(f"[{date_str}] Trovati {total} eventi.")

        # 4. Salvataggio Match Base
        db_module.save_matches_to_db(events, conn=conn)
        
        from tqdm.auto import tqdm
        
        # 5. Loop dettagli (Statistiche, Grafici e Incidenti)
        new_matches_processed = 0
        skipped_matches = 0
        failed_matches = 0
        pbar = tqdm(events, desc=f"Partite {date_str}", unit="match", leave=False)
        for i, event in enumerate(pbar):
            match_id = event['id']
            home_team = event['homeTeam']['name']
            away_team = event['awayTeam']['name']
            
            # --- CONTROLLO ESISTENZA ---
            if db_module.check_match_exists(match_id, conn):
                skipped_matches += 1
                continue
            
            pbar.set_description(f"Data: {date_str} | {home_team} vs {away_team}")
            
            # Scarica dettagli con retry in caso di timeout
            match_success = False
            for attempt in range(MAX_RETRIES):
                try:
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
                    
                    match_success = True
                    break  # Successo, esci dal loop retry
                    
                except Exception as e:
                    logging.warning(f"[{date_str}] Match {match_id} ({home_team} vs {away_team}) tentativo {attempt+1}/{MAX_RETRIES}: {type(e).__name__}: {e}")
                    if attempt < MAX_RETRIES - 1:
                        driver = _restart_driver(driver, headless_mode)
            
            if match_success:
                new_matches_processed += 1
            else:
                failed_matches += 1
                logging.error(f"[{date_str}] Match {match_id} ({home_team} vs {away_team}) SALTATO dopo {MAX_RETRIES} tentativi.")
            
            # Piccolo sleep per cortesia
            time.sleep(0.5)
        pbar.close()
        
        # 6. Aggiorna colonne statistiche (SOLO SE ABBIAMO NUOVI DATI)
        if new_matches_processed > 0:
            logging.info(f"[{date_str}] Rielaborazione colonne statistiche per {new_matches_processed} nuovi match...")
            db_module.populate_statistics_column_db(conn=conn)
        
        logging.info(f"[{date_str}] COMPLETATA — Nuovi: {new_matches_processed}, Già presenti: {skipped_matches}, Falliti: {failed_matches}")
        return True
        
    except Exception as e:
        logging.error(f"[{date_str}] ERRORE CRITICO: {type(e).__name__}: {e}", exc_info=True)
        return False
    finally:
        if driver:
            driver.quit()
        if conn:
            conn.close()
