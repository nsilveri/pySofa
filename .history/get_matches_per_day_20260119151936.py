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

def get_graphics_per_match(match_id, driver):
    url = f'https://www.sofascore.com/api/v1/event/{match_id}/graph'
    driver.get(url)
    
    time.sleep(0.5)  # Ridotto a 0.5 secondi
    
    page_data = driver.page_source
    
    # Estrazione del JSON dal tag <pre>
    json_match = re.search(r'<pre>(.*?)</pre>', page_data, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
        page_json = json.loads(json_str)
        return page_json
    else:
        print(f"No JSON found for match {match_id}")
        return None

def get_statistics_per_match(match_id, driver):
    url = f'https://www.sofascore.com/api/v1/event/{match_id}/statistics'
    driver.get(url)
    
    time.sleep(0.5)
    
    page_data = driver.page_source
    
    # Estrazione del JSON dal tag <pre>
    json_match = re.search(r'<pre>(.*?)</pre>', page_data, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
        page_json = json.loads(json_str)
        return page_json
    else:
        print(f"No JSON found for statistics of match {match_id}")
        return None