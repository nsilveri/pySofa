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
    return None

def get_matches_data(date, driver=None):
    """Ottiene i dati dei match per una data specifica."""
    url = f'https://www.sofascore.com/api/v1/sport/football/scheduled-events/{date}'
    
    close_driver = False
    if driver is None:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)
        close_driver = True
    
    try:
        driver.get(url)
        # Un piccolo sleep potrebbe essere necessario se la pagina carica asincronamente
        time.sleep(1) 
        data = extract_json_from_pre(driver.page_source)
        return data
    except Exception as e:
        logging.error(f"Errore durante il recupero dei dati per la data {date}: {e}")
        return None
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
        logging.error(f"Errore nel recupero dei grafici per match {match_id}: {e}")
        return None

def get_statistics_per_match(match_id, driver):
    """Ottiene le statistiche dettagliate per un match."""
    url = f'https://www.sofascore.com/api/v1/event/{match_id}/statistics'
    try:
        driver.get(url)
        time.sleep(0.5)
        return extract_json_from_pre(driver.page_source)
    except Exception as e:
        logging.error(f"Errore nel recupero delle statistiche per match {match_id}: {e}")
        return None

def get_incidents_per_match(match_id, driver):
    """Ottiene gli incidenti (goal, cartellini, ecc.) per un match."""
    url = f'https://www.sofascore.com/api/v1/event/{match_id}/incidents'
    try:
        driver.get(url)
        time.sleep(0.5)
        return extract_json_from_pre(driver.page_source)
    except Exception as e:
        logging.error(f"Errore nel recupero degli incidenti per match {match_id}: {e}")
        return None