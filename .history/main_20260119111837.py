import sys
sys.path.append('lib')

from selenium import webdriver


driver = webdriver.Chrome()
driver.get('https://www.sofascore.com/api/v1/sport/football/scheduled-events/2026-01-06')

page_data = driver.page_source

print(driver.page_source)
driver.quit()