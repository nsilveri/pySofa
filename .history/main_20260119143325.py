import json
import re
import time

import get_matches_per_day
import db_module
from selenium import webdriver

def print_matches(page_json):
    # Estrazione e stampa dei dati delle partite
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


start_time = time.time()

# Esempio di utilizzo
date = '2024-12-01'
data = get_matches_per_day.get_matches_data(date)
if data:
    #print_matches(data)
    db_module.save_matches_to_db(data['events'])    
    # Ottieni e salva i grafici per ciascuna partita usando un singolo driver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(10)  # Timeout di 10 secondi per il caricamento della pagina
    for i, event in enumerate(data['events']):
        if i >= 5:  # Limita a 5 match per test
            break
        match_id = event['id']
        print(f"Processando match {match_id} ({i+1}/5)")
        graphics = get_matches_per_day.get_graphics_per_match(match_id, driver)
        if graphics:
            db_module.save_graphics_to_db(match_id, graphics)
        else:
            print(f"Nessun grafico trovato per match {match_id}")
        
        # Ottieni e salva le statistiche
        statistics = get_matches_per_day.get_statistics_per_match(match_id, driver)
        if statistics:
            db_module.save_statistics_to_db(match_id, statistics)
        else:
            print(f"Nessuna statistica trovata per match {match_id}")
    driver.quit()
    
    # Popola la tabella statistics_column dopo aver inserito tutti i JSON
    db_module.populate_statistics_column_db()
else:
    print("JSON non trovato nella pagina.")

end_time = time.time()
print(f"Tempo di esecuzione: {end_time - start_time} secondi")