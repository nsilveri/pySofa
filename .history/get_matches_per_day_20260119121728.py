import json
import re
import time

from selenium import webdriver
from selenium.webdriver.common.by import By

def get_matches_data(date):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    url = f'https://www.sofascore.com/api/v1/sport/football/scheduled-events/{date}'
    driver.get(url)
    
    page_data = driver.page_source
    
    # Estrazione del JSON dal tag <pre>
    json_match = re.search(r'<pre>(.*?)</pre>', page_data, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
        page_json = json.loads(json_str)
        driver.quit()
        return page_json
    else:
        driver.quit()
        return None

def get_graphics_per_match(match_id):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    url = f'https://www.sofascore.com/api/v1/event/{match_id}/graph'
    driver.get(url)
    
    time.sleep(1)  # Attendi che la pagina si carichi completamente
    
    page_data = driver.page_source
    
    # Estrazione del JSON dal tag <pre>
    json_match = re.search(r'<pre>(.*?)</pre>', page_data, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
        page_json = json.loads(json_str)
        driver.quit()
        return page_json
    else:
        print(f"No JSON found for match {match_id}, page starts with: {page_data[:200]}")
        driver.quit()
        return None