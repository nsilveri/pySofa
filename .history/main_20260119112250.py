import sys
sys.path.append('lib')
import json

from selenium import webdriver
from selenium.webdriver.common.by import By


driver = webdriver.Chrome()
driver.get('https://www.sofascore.com/api/v1/sport/football/scheduled-events/2026-01-06')

page_data = driver.page_source

page_json = json.loads(page_data)
print(json.dumps(page_json, indent=4))
driver.quit()