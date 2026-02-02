import sys
sys.path.append('lib')
import json

from selenium import webdriver
from selenium.webdriver.common.by import By


driver = webdriver.Chrome()
driver.get('https://www.sofascore.com/api/v1/sport/football/scheduled-events/2026-01-06')

page_data = driver.page_source

try:
    page_json = json.loads(page_data)
    
    # Estrazione e stampa dei dati delle partite
    for event in page_json:
        home_team = event['homeTeam']['name']
        away_team = event['awayTeam']['name']
        home_score = event['homeScore']['current']
        away_score = event['awayScore']['current']
        status = event['status']['description']
        tournament = event['tournament']['name']
        
        print(f"Torneo: {tournament}")
        print(f"{home_team} {home_score} - {away_score} {away_team} ({status})")
        print("-" * 50)
except json.JSONDecodeError:
    print("Errore nel parsing JSON. Contenuto della pagina:")
    print(page_data[:1000])

driver.quit()