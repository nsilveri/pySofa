import sys
sys.path.append('lib')

import requests

# Statistiche partite
url = 'https://www.sofascore.com/api/v1/sport/football/scheduled-events/2026-01-06'

response = requests.get(url)
data = response.json()

print(data)