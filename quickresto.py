from datetime import date, datetime, time, timedelta
from json.decoder import JSONDecodeError
from typing import Tuple

import pandas as pd
import requests

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

    def __init__(self, server_data: dict, module_settings: dict):
        self.server_data = server_data
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

        return response

    def select_df_colummns(self, df_input: pd.DataFrame,
                           module_fields: list, **kwargs) -> pd.DataFrame:
        df = df_input.copy()
        if (len(module_fields) > 0) & (len(df.columns) > 0):
            df = df[module_fields]
        return df


class QROrderDayData(QRData):

    parts = 10

    def __init__(self, day: date, **kwargs):
        super().__init__(**kwargs)
        self.day = day

    def convert_to_javatime(self, d: datetime.date) -> int:
        timestamp = int((d - date(1970, 1, 1)) / timedelta(seconds=1)) * 1000
        return timestamp

    def get_filter(self, module_date_field: str,
                   since: int, till: int,
                   **kwargs):

        filter = {"filters":
                  [{"field": module_date_field,
                   "operation": "range",
                    "value": {"since": since, "till": till}}
                   ]}
        return filter

    def log_response(self, response):
        display_len = 170
        if len(response.text) > display_len:
            end_line = '...]'
        else:
            end_line = ''
        print(response.status_code,
              response.text.replace('\n', '')[:display_len], end_line)

    def proc_response(self, response: requests.Response) -> pd.DataFrame:
        self.log_response(response)
        json_response = response.json()
        df = pd.DataFrame.from_dict(json_response)
        return df

    def get_parts_data(self, response,
                       since: int, till: int, parts: int) -> pd.DataFrame:
        self.log_response(response)
        print('[retry with time parts...]')

        df = pd.DataFrame([])
        try:
            step = (till - since) // parts
            p_since, p_till = since, since + step
            while p_till <= till:
                if p_till > till:
                    p_till = till
                filter = self.get_filter(since=p_since, till=p_till,
                                         **self.module_settings)
                response = self.get_api_df(filter=filter, **self.server_data,
                                           **self.module_settings)
                df_part = self.proc_response(response)  # try part
                df = df.append(df_part, ignore_index=True)
                p_since, p_till = p_till + 1, p_till + step
        except JSONDecodeError:
            print('[JSONDecodeError]')
            df = pd.DataFrame([])
        return df

    def get_data(self) -> pd.DataFrame:
        """ Collect and return DataFrame """

        since = self.convert_to_javatime(self.day)
        till = self.convert_to_javatime(self.day + timedelta(days=1))

        filter = self.get_filter(since=since, till=till,
                                 **self.module_settings)
        response = self.get_api_df(filter=filter, **self.server_data,
                                   **self.module_settings)

        if 500 <= response.status_code < 600:
            df = self.get_parts_data(response, since=since,
                                     till=till, parts=self.parts)
        else:
            try:
                df = self.proc_response(response)
            except JSONDecodeError:
                print('[JSONDecodeError]')
                df = pd.DataFrame([])

        # drop columns
        df = self.select_df_colummns(df, **self.module_settings)
        df['server_name'] = self.server_data.get('server_name')
        return df


class OrderDayReport():

    module_settings = {
        'module_name': 'front.orders',
        'module_date_field': 'localCreateDate',
        'module_fields': ['createDate', 'localCreateDate',
                          'createTerminalSalePlace',
                          'returned', 'frontTotalPrice',
                          'id', 'payments'],
        'sale_place_field': 'createTerminalSalePlaceDocId'
    }

    def __init__(self):
        pass
        # self.server_data = server_data
        # self.day = day

    def load_data(self, server_data: dict, day: date):
        qr = QROrderDayData(server_data=server_data,
                            module_settings=self.module_settings,
                            day=day)
        df = qr.get_data()
        return df

    def write_xlsx(self, df: pd.DataFrame, path: str = 'out.xlsx'):
        writer = pd.ExcelWriter(path)
        df.to_excel(writer, index=False, sheet_name='1')
        writer.save()
        print('=>', path)

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
            df['localCreateDate'] = pd.to_datetime(
                df['localCreateDate']).dt.date
            df['place'] = df['createTerminalSalePlace'].apply(
                lambda x: x.get('title'))

            df['totalPrice'] = df['payments'].apply(self.fiscal_sum)

            df = df.groupby(['place', 'localCreateDate', 'server_name']
                            )['totalPrice'].agg(['sum', 'mean', 'count']
                                                ).reset_index()
            df = df.rename(columns={'place': 'Place',
                                    'localCreateDate': 'Date',
                                    'sum': 'Revenue',
                                    'count': 'NumberOfOrders',
                                    'mean': 'AverageOrder',
                                    'server_name': 'ServerName'
                                    })
        else:
            df = pd.DataFrame([])
        return df

    def get_xlsx_data(self, server_data: dict, day: date,
                      path: str = 'out.xlsx'):
        df = self.load_data(server_data=server_data, day=day)
        # df['localCreateDate'] = pd.to_datetime(
        # df['localCreateDate']).dt.date
        # df['place'] = df['createTerminalSalePlace'].apply(
        # lambda x: x.get('title'))
        df['totalPrice'] = df['payments'].apply(self.fiscal_sum)
        df.drop(columns=['createTerminalSalePlace'], inplace=True)
        self.write_xlsx(df, path=path)

    def get_report(self, server_data: dict, day: date):
        df_input = self.load_data(server_data=server_data, day=day)
        df = self.proc_data(df_input)
        return df


if __name__ == '__main__':
    pass
    # one_day_date = (2021, 9, 11)
    # report = OrderDayReport()
    # df_front_orders = report.get_report(server_data=QR_SERVERS[0],
    #                                     day=date(*one_day_date))
    # print(df_front_orders)
