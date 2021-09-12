from datetime import date, datetime, time, timedelta

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from requests.auth import HTTPBasicAuth


class QuickResto():

    def __init__(self, servers_data: list,
                 module_name: str,
                 module_date_field: str,
                 nubmer_of_months: int,
                 days_time_step: int):

        self.module_name = module_name
        self.module_date_field = module_date_field
        self.servers_data = servers_data
        self.init_dates(nubmer_of_months, days_time_step)
        self.headers = {'Content-Type': "application/json"}
        self.df = pd.DataFrame([])

    def init_dates(self, nubmer_of_months: int, days_time_step: int):
        def convert_to_javatime(d: date) -> int:
            return int(datetime.combine(d, time()).timestamp()*1000)

        end_date = date.today()
        self.end_date = convert_to_javatime(end_date)

        start_current_month = end_date.replace(day=1)
        # first day of month
        if end_date.day == 1:
            nubmer_of_months += 1
        start_date = (start_current_month -
                      relativedelta(months=nubmer_of_months))
        self.start_date = convert_to_javatime(start_date)

        next_date = end_date + relativedelta(days=days_time_step)
        self.time_step = convert_to_javatime(next_date) - self.end_date

    def get_df_batch(self,
                     server_name: str,
                     module: str,
                     user: str, password: str,
                     filter: dict = {}) -> pd.DataFrame:
        url = f"https://{server_name}.quickresto.ru/platform/online/api/list?moduleName={module}"
        querystring = filter
        response = requests.request("GET", url, headers=self.headers,
                                    json=querystring,
                                    auth=HTTPBasicAuth(user, password))
        json_response = response.json()
        # print(json_response)
        df = pd.DataFrame.from_dict(json_response)
        return df

    def get_filter(self, field, since, till):
        filter = {"filters":
                  [{"field": field,
                    "operation": "range",
                    "value": {"since": since, "till": till}}
                   ]}
        return filter

    def get_time_parts(self, server_data: dict) -> pd.DataFrame:

        server_name = server_data.get('server_name')
        user = server_data.get('user')
        password = server_data.get('password')
        module = self.module_name
        field = self.module_date_field
        since = self.start_date

        df = pd.DataFrame([])
        while since < self.end_date:
            till = since + self.time_step
            if till > self.end_date:
                till = self.end_date
            filter = self.get_filter(field, since, till)
            df_batch = self.get_df_batch(server_name, module,
                                         user, password, filter)
            df = df.append(df_batch, ignore_index=True)
            since = till

        return df

    def get_servers_parts(self):
        for server_data in self.servers_data:
            server_name = server_data.get('server_name')
            df = self.get_time_parts(server_data)
            df['server_name'] = server_name
            self.df = self.df.append(df, ignore_index=True)

    def get_data(self) -> pd.DataFrame:
        """ Collect and return DataFrame """
        self.df = pd.DataFrame([])
        self.get_servers_parts()
        return self.df


SERVERS = [{'server_name': '',
            'user': '',
            'password': ''
            },
           {'server_name': '',
            'user': '',
            'password': ''
            }
           ]

NUMBER_OF_MONTHS = 0
DAYS_STEP = 1

# qr_modules = [{'module_name': 'front.orders',
#             'module_date_field': 'createDate'
#             },
#             {'module_name': 'warehouse.documents.incoming',
#             'module_date_field': 'invoiceDate'
#             }]


if __name__ == '__main__':

    # warehouse.documents.incoming
    module_name = 'warehouse.documents.incoming'
    module_date_field = 'invoiceDate'

    df_docs_incoming = QuickResto(servers_data=SERVERS,
                                  module_name=module_name,
                                  module_date_field=module_date_field,
                                  nubmer_of_months=NUMBER_OF_MONTHS,
                                  days_time_step=DAYS_STEP).get_data()

    # front.orders
    module_name = 'front.orders'
    module_date_field = 'createDate'

    df_front_orders = QuickResto(servers_data=SERVERS,
                                 module_name=module_name,
                                 module_date_field=module_date_field,
                                 nubmer_of_months=NUMBER_OF_MONTHS,
                                 days_time_step=DAYS_STEP).get_data()
