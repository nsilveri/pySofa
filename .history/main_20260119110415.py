import sys
sys.path.append('lib')

from selenium import webdriver


driver = webdriver.Chrome()
driver.get('https://selenium.dev/')
pr
driver.quit()