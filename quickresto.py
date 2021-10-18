from datetime import date, datetime, time, timedelta
from typing import Tuple

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

# from setup import QR_SERVERS

# Setup

# array of server data dicts
# SERVERS = [{'server_name': '',
#             'user': '',
#             'password': ''
#             },
#            {'server_name': '',
#             'user': '',
#             'password': ''
#             },
#            ]


class QRData():

    def __init__(self, servers_data: list, module_settings: dict):
        self.servers_data = servers_data
        self.module_settings = module_settings

    def get_api_df(self, server_name: str, user: str, password: str,
                   module_name: str, filter: dict = {},
                   **kwargs) -> pd.DataFrame:

        url = (f"https://{server_name}.quickresto.ru/platform/online/api/"
               f"list?moduleName={module_name}")
        querystring = filter
        headers = {'Content-Type': "application/json"}

        response = requests.request(
            "GET", url, headers=headers,
            json=querystring,
            auth=requests.auth.HTTPBasicAuth(user, password))
        print(response.status_code, response.text[:200])
        json_response = response.json()
        df = pd.DataFrame.from_dict(json_response)
        return df

    def filter_df_colummns(self, df_input: pd.DataFrame,
                           module_fields: list, **kwargs) -> pd.DataFrame:
        df = df_input.copy()
        if (len(module_fields) > 0) & (len(df.columns) > 0):
            df = df[module_fields]
        return df

    def get_filter(self, **kwargs):
        return {}

    def get_servers_parts(self):
        df = pd.DataFrame([])
        kwargs = {'filter': self.get_filter(**self.module_settings),
                  **self.module_settings}

        for server_data in self.servers_data:
            df_part = self.get_api_df(**server_data, **kwargs)
            # drop columns
            df_part = self.filter_df_colummns(df_part, **self.module_settings)
            df_part['server_name'] = server_data.get('server_name')
            df = df.append(df_part, ignore_index=True)
        return df

    def get_data(self) -> pd.DataFrame:
        """ Collect and return DataFrame """
        df = self.get_servers_parts()
        return df


class QROrderDayData(QRData):

    def __init__(self, day: date, **kwargs):
        super().__init__(**kwargs)
        self.day = day

    def convert_to_javatime(self, d: datetime.date) -> int:
        timestamp = int((d - date(1970, 1, 1)) / timedelta(seconds=1)) * 1000
        return timestamp

    def get_filter(self, module_date_field: str, **kwargs):
        since = self.convert_to_javatime(self.day)
        till = self.convert_to_javatime(self.day + timedelta(days=1))

        filter = {"filters":
                  [{"field": module_date_field,
                   "operation": "range",
                    "value": {"since": since, "till": till}}
                   ]}
        return filter


class OrderDayReport():

    module_settings = {
        'module_name': 'front.orders',
        'module_date_field': 'localCreateDate',
        'module_fields': ['createDate', 'createTerminalSalePlace',
                          'returned', 'frontTotalPrice',
                          'id', 'payments'],
        'sale_place_field': 'createTerminalSalePlaceDocId'
    }

    def __init__(self, servers_data: list, day: date):
        self.servers_data = servers_data
        self.day = day

    def load_data(self):
        qr = QROrderDayData(servers_data=self.servers_data,
                            module_settings=self.module_settings,
                            day=self.day)
        df = qr.get_data()
        return df

    def fiscal_sum(self, payments_list: list) -> float:
        """Sum of order fiscal operations"""
        fiscal_sum = 0
        for item in payments_list:
            operationType = item.get('operationType', '')
            amount = item.get('amount', 0)
            if operationType == 'fiscal':
                fiscal_sum += amount
        return fiscal_sum

    def proc_data(self, df_input) -> pd.DataFrame:
        """Data preprocessing"""

        if len(df_input) > 0:
            df = df_input.copy()
            df = df[df['returned'] == False]
            df['createDate'] = pd.to_datetime(
                df['createDate']).dt.date
            df['place'] = df['createTerminalSalePlace'].apply(
                lambda x: x.get('title'))

            df['totalPrice'] = df['payments'].apply(self.fiscal_sum)

            df = df.groupby(['place', 'createDate']
                            )['totalPrice'].agg(['sum', 'mean', 'count']
                                                ).reset_index()
            df = df.rename(columns={'place': 'Place',
                                    'createDate': 'Date',
                                    'sum': 'Revenue',
                                    'count': 'NumberOfOrders',
                                    'mean': 'AverageOrder'
                                    })
        else:
            df = pd.DataFrame([])
        return df

    def get_report(self):
        df_input = self.load_data()
        df = self.proc_data(df_input)
        return df


if __name__ == '__main__':
    pass
    # one_day_date = (2021, 9, 11)
    # report = OrderDayReport(servers_data=QR_SERVERS,
    #                         day=date(*one_day_date))
    # df_front_orders = report.get_report()
    # df_front_orders
