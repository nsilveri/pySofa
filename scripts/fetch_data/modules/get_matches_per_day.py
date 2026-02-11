import json
import re
import time
import logging
from selenium import webdriver

def extract_json_from_pre(page_source):
    """Estrae e carica il JSON dal tag <pre> della pagina."""
    json_match = re.search(r'<pre>(.*?)</pre>', page_source, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError as e:
            logging.error(f"Errore nella decodifica JSON: {e}")
            logging.debug(f"Contenuto raw (primi 500 char): {json_match.group(1)[:500]}")
    else:
        # Log diagnostico: cosa c'è nella pagina se non troviamo <pre>?
        snippet = page_source[:500] if page_source else "(pagina vuota)"
        logging.warning(f"Tag <pre> non trovato nella risposta. Snippet pagina: {snippet}")
    return None

def get_matches_data(date, driver=None):
    """Ottiene i dati dei match per una data specifica.
    
    NOTA: questa funzione NON cattura le eccezioni di Selenium internamente,
    così il chiamante può gestire i retry.
    """
    url = f'https://www.sofascore.com/api/v1/sport/football/scheduled-events/{date}'
    
    close_driver = False
    if driver is None:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)
        close_driver = True
    
    try:
        logging.debug(f"GET {url}")
        driver.get(url)
        time.sleep(1)
        
        # Log diagnostico: stato della pagina
        current_url = driver.current_url
        page_len = len(driver.page_source) if driver.page_source else 0
        logging.debug(f"Pagina caricata: URL={current_url}, dimensione={page_len} chars")
        
        data = extract_json_from_pre(driver.page_source)
        
        if data is None:
            logging.warning(f"Nessun JSON estratto per data {date}")
        elif 'events' not in data:
            logging.warning(f"JSON ricevuto per {date} ma senza 'events'. Chiavi: {list(data.keys())}")
        else:
            logging.debug(f"JSON OK per {date}: {len(data.get('events', []))} eventi trovati")
        
        return data
    except Exception as e:
        logging.error(f"Errore Selenium per data {date}: {type(e).__name__}: {e}")
        # Ri-lanciamo l'eccezione per permettere al chiamante di gestire i retry
        raise
    finally:
        if close_driver:
            driver.quit()

def get_graphics_per_match(match_id, driver):
    """Ottiene i grafici (possessione/pressione) per un match."""
    url = f'https://www.sofascore.com/api/v1/event/{match_id}/graph'
    try:
        driver.get(url)
        time.sleep(0.5)
        return extract_json_from_pre(driver.page_source)
    except Exception as e:
        logging.error(f"Errore nel recupero dei grafici per match {match_id}: {type(e).__name__}: {e}")
        raise

def get_statistics_per_match(match_id, driver):
    """Ottiene le statistiche dettagliate per un match."""
    url = f'https://www.sofascore.com/api/v1/event/{match_id}/statistics'
    try:
        driver.get(url)
        time.sleep(0.5)
        return extract_json_from_pre(driver.page_source)
    except Exception as e:
        logging.error(f"Errore nel recupero delle statistiche per match {match_id}: {type(e).__name__}: {e}")
        raise

def get_incidents_per_match(match_id, driver):
    """Ottiene gli incidenti (goal, cartellini, ecc.) per un match."""
    url = f'https://www.sofascore.com/api/v1/event/{match_id}/incidents'
    try:
        driver.get(url)
        time.sleep(0.5)
        return extract_json_from_pre(driver.page_source)
    except Exception as e:
        logging.error(f"Errore nel recupero degli incidenti per match {match_id}: {type(e).__name__}: {e}")
        raise