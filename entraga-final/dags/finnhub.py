import requests

# API Class
class Finnhub:
  BASE_URL="https://finnhub.io/api/v1"

  def __init__(self, token):
    self.token = token
  
  def quote(self, symbol):
    params = { "symbol": symbol, "token": self.token }
    res = requests.get(f'{self.BASE_URL}/quote', params=params)
    res.raise_for_status()
    return res.json()
