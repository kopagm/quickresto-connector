from requests.models import Response
from api.qr_order import QROrder
from datetime import date, datetime, timedelta
import pandas as pd
import requests
from json.decoder import JSONDecodeError
import sys


class QROrderData(QROrder):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parts = 10
        self.parts_max = 120
        self.parts_step = 30
        self.retry_limit = 5
        # self.auth = requests.auth.HTTPBasicAuth(
        #     self.server_data['user'], self.server_data['password'])
        # self.headers = {'Content-Type': "application/json"}
        self.url = (f"https://{self.server_data['server_name']}."
                    f"quickresto.ru/platform/online/api/list?"
                    f"moduleName={self.module_settings['module_name']}")
        self.session = self.get_session()

    def get_session(self):
        session = requests.Session()
        session.auth = (self.server_data['user'], self.server_data['password'])
        session.headers.update({'Content-Type': "application/json"})
        return session

    def proc_response(self, response: Response) -> dict:
        code = response.status_code
        if code == 200:
            try:
                json = response.json()
            except JSONDecodeError:
                raise ValueError(code, 'JSONDecodeError', response.text)
            else:
                result = {'code': response.status_code,
                          'json': json}
        else:
            raise ValueError(code, response.text)
        return result

    def get_api(self, filter: dict = {}) -> dict:
        response = self.session.get(self.url, json=filter)
        # response = requests.request("GET", self.url, headers=self.headers,
        #                             json=filter, auth=self.auth)
        result = self.proc_response(response=response)
        return result

    def convert_to_javatime(self, d: datetime.date) -> int:
        timestamp = int((d - date(1970, 1, 1)) /
                        timedelta(seconds=1)) * 1000
        return timestamp

    def get_filter(self, since: int, till: int) -> dict:
        filter = {"filters":
                  [{"field": self.module_settings['module_date_field'],
                   "operation": "range",
                    "value": {"since": since, "till": till}}
                   ]}
        return filter

    def get_parts(self, day: date) -> pd.DataFrame:
        print(f'[{datetime.now().strftime("%m.%d.%Y %H:%M:%S")}] '
              f'[{self.server_data["server_name"]}] [{day}] '
              f'[try with {self.parts} parts...]')
        sys.stdout.flush()

        df = pd.DataFrame([])

        since = self.convert_to_javatime(day)
        till = self.convert_to_javatime(day + timedelta(days=1))
        step = (till - since) // self.parts
        p_since, p_till = since, since + step
        while p_till <= till:
            if p_till > till:
                p_till = till
            filter = self.get_filter(since=p_since, till=p_till)
            response_dict = self.get_api(filter=filter)['json']
            df_part = pd.DataFrame.from_dict(response_dict)
            df = df.append(df_part, ignore_index=True)
            p_since, p_till = p_till + 1, p_till + step
        return df

    def get_data(self, day: date) -> pd.DataFrame:
        result = None
        retry = 0
        while result is None:
            try:
                result = self.get_parts(day=day)
            except ValueError as error:
                if error.args[0] == 504:
                    if self.parts + self.parts_step <= self.parts_max:
                        self.parts += self.parts_step
                    else:
                        print(f'[{datetime.now().strftime("%m.%d.%Y %H:%M:%S")}] '
                              f'[{self.server_data["server_name"]}] [{day}] '
                              f'[Fails with {self.parts} parts]')
                        result = pd.DataFrame([])
                elif retry < self.retry_limit:
                        retry +=1
                else:
                    print(f'[Error] {error.args}')
                    result = pd.DataFrame([])
                    # break
        return result

    def select_df_colummns(self, df_input: pd.DataFrame) -> pd.DataFrame:
        df = df_input.copy()
        module_fields = self.module_settings['module_fields']
        if len(module_fields) & len(df.columns):
            df = df[module_fields]
        return df

    def get(self, day: date) -> pd.DataFrame:
        # print(f'QROrderData {day}')
        df = self.get_data(day)
        # drop columns
        df = self.select_df_colummns(df)
        # df['server_name'] = self.server_data.get('server_name')
        return df
