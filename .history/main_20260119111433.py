import sys
sys.path.append('lib')

from selenium import webdriver

driver = webdriver.Chrome()

# Statistiche partite
driver.get('https://www.sofascore.com/api/v1/sport/football/scheduled-events/2026-01-06')

# Dati
dati = driver.page_source
print(dati)
driver.quit()