from datetime import date, datetime, time, timedelta

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


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

# number of months to collect. 0 - only current month
NUMBER_OF_MONTHS = 0

# time window in days for api call
DAYS_STEP = 5


class QuickRestoData():

    MODULE_NAME_FRONT_ORDERS = 'front.orders'

    def __init__(self, servers_data: list,
                 #  module_name: str,
                 #  module_date_field: str,
                 module_settings: dict,
                 nubmer_of_months: int,
                 days_time_step: int):

        self.module_name = module_settings.get('module_name')
        self.module_date_field = module_settings.get('module_date_field')
        self.module_fields = module_settings.get('module_fields', [])
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

        self.time_step = days_time_step * 60 * 60 * 24 * 1000

    def get_df_batch(self, server_data: dict,
                     filter: dict = {}) -> pd.DataFrame:

        server_name = server_data.get('server_name')
        user = server_data.get('user')
        password = server_data.get('password')
        module = self.module_name

        url = f"https://{server_name}.quickresto.ru/platform/online/api/list?moduleName={module}"
        querystring = filter
        response = requests.request("GET", url, headers=self.headers,
                                    json=querystring,
                                    auth=requests.auth.HTTPBasicAuth(user,
                                                                     password))
        json_response = response.json()
        df = pd.DataFrame.from_dict(json_response)
        return df

    def get_filter(self, field, since, till):
        filter = {"filters":
                  [{"field": field,
                    "operation": "range",
                    "value": {"since": since, "till": till}}
                   ]}
        return filter

    def filter_df_colummns(self, df_input: pd.DataFrame) -> pd.DataFrame:
        if (len(self.module_fields) > 0) & (len(df_input.columns) > 0):
            df = df_input[self.module_fields]
        else:
            df = df_input
        return df

    def get_time_parts(self, server_data: dict) -> pd.DataFrame:

        field = self.module_date_field
        since = self.start_date

        df = pd.DataFrame([])
        while since < self.end_date:
            till = since + self.time_step
            if till > self.end_date:
                till = self.end_date
            filter = self.get_filter(field, since, till)

            # get part of api table
            df_batch = self.get_df_batch(server_data, filter)

            # drop columns
            df_batch = self.filter_df_colummns(df_batch)

            # add new data to DataFrame
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


class OrderReport():

    module_settings = {
        'module_name': 'front.orders',
        'module_date_field': 'createDate',
        'module_fields': ['createDate', 'createTerminalSalePlace',
                          'returned', 'frontTotalPrice']
    }

    def __init__(self, servers_data: list,
                 nubmer_of_months: int,
                 days_time_step: int):

        self.servers_data = servers_data
        self.nubmer_of_months = nubmer_of_months
        self.days_time_step = days_time_step
        self.df = pd.DataFrame([])

    def load_data(self):

        self.df = QuickRestoData(servers_data=self.servers_data,
                                 module_settings=self.module_settings,
                                 nubmer_of_months=NUMBER_OF_MONTHS,
                                 days_time_step=DAYS_STEP).get_data()

    def proc_data(self):
        df = self.df.copy()
        df = df[df['returned'] == False]
        df['createDate'] = pd.to_datetime(
            df['createDate']).dt.date
        df['place'] = df['createTerminalSalePlace'].apply(
            lambda x: x.get('title'))

        df = df.groupby(['place', 'createDate'])['frontTotalPrice'].agg(
            ['sum', 'mean', 'count']).reset_index()
        df = df.rename(columns={'place': 'Место реализации',
                           'createDate': 'Дата продажи',
                           'sum': 'Выручка',
                           'count': 'Кол-во чеков',
                           'mean': 'Средний чек'
                           })
        self.df = df

    def get_report(self):
        self.load_data()
        self.proc_data()
        return self.df


if __name__ == '__main__':

    df_front_orders = OrderReport(servers_data=SERVERS,
                                nubmer_of_months=NUMBER_OF_MONTHS,
                                days_time_step=DAYS_STEP).get_report()
    df_front_orders
