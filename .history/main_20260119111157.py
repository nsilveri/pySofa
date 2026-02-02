import sys
sys.path.append('lib')

import requests

# Statistiche partite
url = 'https://www.sofascore.com/api/v1/sport/football/scheduled-events/2026-01-06'

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
response = requests.get(url, headers=headers)
data = response.json()

print(data)