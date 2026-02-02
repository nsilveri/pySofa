import sys
sys.path.append('lib')
import json
import re
import time

import get_matches_per_day

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
date = '2026-01-06'
data = get_matches_per_day.get_matches_data(date)
if data:
    print_matches(data)
else:
    print("JSON non trovato nella pagina.")