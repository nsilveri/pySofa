import sys
sys.path.append('lib')

from selenium import webdriver


driver = webdriver.Chrome()
driver.get('https://www.sofascore.com/api/v1/sport/football/scheduled-events/2026-01-06')

page_data = driver.page_source
page_data = driver.find_element_by_tag_name('pre').text

page_json =  json.loads(content)
print(driver.page_source)
driver.quit()