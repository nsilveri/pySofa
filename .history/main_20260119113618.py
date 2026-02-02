import sys
sys.path.append('lib')
import json
import re

from selenium import webdriver
from selenium.webdriver.common.by import By


driver = webdriver.Chrome()
driver.get('https://www.sofascore.com/api/v1/sport/football/scheduled-events/2026-01-06')

page_data = driver.page_source

# Estrazione del JSON dal tag <pre>
json_match = re.search(r'<pre>(.*?)</pre>', page_data, re.DOTALL)
if json_match:
    json_str = json_match.group(1)
    page_json = json.loads(json_str)
    
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
else:
    print("JSON non trovato nella pagina.")

driver.quit()