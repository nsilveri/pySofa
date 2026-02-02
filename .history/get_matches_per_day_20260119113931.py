def get_matches_data(date):
    driver = webdriver.Chrome()
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