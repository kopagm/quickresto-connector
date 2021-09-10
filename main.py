import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

class QuickResto():

  def __init__(self, server_name, user, password):
    self.server_name = server_name
    self.user = user
    self.password = password
    self.headers = {'Content-Type': "application/json"}

  def get(self, module: str, filter: dict={}) -> pd.DataFrame:
    url = f"https://{self.server_name}.quickresto.ru/platform/online/api/list?moduleName={module}"
    querystring = filter

    response = requests.request("GET", url, headers=self.headers,
                                params=querystring,
                                auth=HTTPBasicAuth(self.user, self.password))
    json_response = response.json()
    df = pd.DataFrame.from_dict(json_response)
    # print(df)
    return df

  def get_store(self) -> pd.DataFrame:
    module = 'warehouse.store'
    df = self.get(module)
    return df

  def get_incoming(self) -> pd.DataFrame:
    module = 'warehouse.documents.incoming'
    df = self.get(module)
    return df

SERVERS = [{'server_name': '',
  'user': '',
  'password': ''
  }]

def main():
  server = SERVERS[0]
  qr = QuickResto(**server)
  df = qr.get_incoming()
  return df

if __name__ == '__main__':
    # print('main')
    df = main()
df