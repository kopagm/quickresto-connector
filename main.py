from datetime import date, datetime, time, timedelta
from typing import Tuple

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
DAYS_STEP = 1


class QRData():

    def __init__(self, servers_data: list, module_settings: dict):

        self.module_name = module_settings.get('module_name')
        self.module_date_field = module_settings.get('module_date_field')
        self.module_fields = module_settings.get('module_fields', [])
        self.servers_data = servers_data
        # self.init_dates(nubmer_of_months, days_time_step)
        self.headers = {'Content-Type': "application/json"}
        self.set_blank_df()

    def set_blank_df(self):
        self.df = pd.DataFrame([])

    def get_api_df(self, filter: dict = {}) -> pd.DataFrame:

        server_name, user, password, module = (
            self.server_name, self.user, self.password, self.module_name)

        url = f"https://{server_name}.quickresto.ru/platform/online/api/list?moduleName={module}"
        querystring = filter
        response = requests.request("GET", url, headers=self.headers,
                                    json=querystring,
                                    auth=requests.auth.HTTPBasicAuth(user,
                                                                     password))
        print(response.status_code, response.text[:200])
        json_response = response.json()
        df = pd.DataFrame.from_dict(json_response)
        return df

    def filter_df_colummns(self, df_input: pd.DataFrame) -> pd.DataFrame:
        if (len(self.module_fields) > 0) & (len(df_input.columns) > 0):
            df = df_input[self.module_fields]
        else:
            df = df_input
        return df

    def append_df(self, df_input: pd.DataFrame):
        self.df = self.df.append(df_input, ignore_index=True)

    def get_server_part(self) -> pd.DataFrame:
        return self.get_api_df()

    def get_servers_parts(self):
        for server_data in self.servers_data:
            (self.server_name,
             self.user,
             self.password) = (server_data.get('server_name'),
                               server_data.get('user'),
                               server_data.get('password'))

            df = self.get_server_part()
            # drop columns
            df = self.filter_df_colummns(df)
            df['server_name'] = self.server_name
            self.append_df(df)

    def get_data(self) -> pd.DataFrame:
        """ Collect and return DataFrame """
        self.set_blank_df()
        self.get_servers_parts()
        return self.df


class QROrderData(QRData):

    def __init__(self, servers_data: list,
                 module_settings: dict,
                 nubmer_of_months: int = 0,
                 days_time_step: int = 1,
                 one_day_date: Tuple[int, int, int] = (2021, 9, 14),
                 sale_place_id: str = ''
                 ):

        super().__init__(servers_data, module_settings)

        self.nubmer_of_months = nubmer_of_months
        self.days_time_step = days_time_step * 60 * 60 * 24 * 1000
        self.one_day_date = one_day_date
        self.sale_place_field = module_settings.get('sale_place_field', '')
        self.sale_place_id = sale_place_id

        self.init_dates()

    def convert_to_javatime(self, d: date) -> int:
        return int(datetime.combine(d, time()).timestamp()*1000)

    def init_dates(self):
        end_date = date.today()
        self.end_date = self.convert_to_javatime(end_date)

        start_current_month = end_date.replace(day=1)
        # first day of month
        if end_date.day == 1:
            self.nubmer_of_months += 1
        start_date = (start_current_month -
                      relativedelta(months=self.nubmer_of_months))
        self.start_date = self.convert_to_javatime(start_date)

    def get_filter(self):
        filter = {"filters":
                  [{"field": self.module_date_field,
                   "operation": "range",
                    "value": {"since": self.since, "till": self.till}}
                   ]}
        return filter

    def get_server_part(self) -> pd.DataFrame:

        # field = self.module_date_field
        self.since = self.start_date

        df = pd.DataFrame([])
        while self.since < self.end_date:
            self.till = self.since + self.days_time_step
            if self.till > self.end_date:
                self.till = self.end_date
            filter = self.get_filter()

            # get part of api table
            df_batch = self.get_api_df(filter=filter)

            # drop columns
            df_batch = self.filter_df_colummns(df_batch)

            # add new data to DataFrame
            df = df.append(df_batch, ignore_index=True)
            self.since = self.till

        return df


class QROrderOneDayData(QROrderData):

    def init_dates(self):
        year, month, day = self.one_day_date
        end_date = date(year, month, day+1)
        self.end_date = self.convert_to_javatime(end_date)

        start_date = date(year, month, day)
        self.start_date = self.convert_to_javatime(start_date)


class QROrderOneSalePlaceData(QROrderData):

    def get_filter(self):
        if self.sale_place_id == '' or self.sale_place_field == '':
            filter = super().get_filter()
        else:
            filter = {"filters":
                      [{"field": self.module_date_field,
                        "operation": "range",
                        "value": {"since": self.since, "till": self.till}},
                       {"field": self.sale_place_field,
                          "operation": "eq",
                          "value": self.sale_place_id}
                       ]}
        return filter


class QROrderOneDaySalePlaceData(QROrderOneSalePlaceData,
                                 QROrderOneDayData):
    pass


class OrderReport():

    module_settings = {
        'module_name': 'front.orders',
        'module_date_field': 'createDate',
        'module_fields': ['createDate', 'createTerminalSalePlace',
                          'returned', 'frontTotalPrice',
                          'id', 'payments'],
        'sale_place_field': 'createTerminalSalePlaceDocId'
    }

    def __init__(self, servers_data: list,
                 nubmer_of_months: int = 0,
                 days_time_step: int = 1,
                 one_day_date: Tuple[int, int, int] = (2021, 9, 14),
                 sale_place=''):

        self.servers_data = servers_data
        self.nubmer_of_months = nubmer_of_months
        self.days_time_step = days_time_step
        self.one_day_date = one_day_date
        self.sale_place = sale_place
        self.df = pd.DataFrame([])

    def load_data(self):

        qr = QROrderData(servers_data=self.servers_data,
                         module_settings=self.module_settings,
                         nubmer_of_months=self.nubmer_of_months,
                         days_time_step=self.days_time_step)
        self.df_input = qr.get_data()

    def write_xlsx(self, df: pd.DataFrame, path: str = 'out.xlsx'):
        writer = pd.ExcelWriter(path)
        df.to_excel(writer, index=False, sheet_name='1')
        writer.save()
        print('=>', path)

    def proc_data(self):
        """Data preprocessing"""

        def fiscal_sum(payments_list: list) -> float:
            """Sum of order fiscal operations"""
            fiscal_sum = 0
            for item in payments_list:
                operationType = item.get('operationType', '')
                amount = item.get('amount', 0)
                if operationType == 'fiscal':
                    fiscal_sum += amount
            return fiscal_sum

        def prepayment_sum(payments_list: list) -> float:
            """Sum of prepayment in order payment data"""
            prepayment_sum = 0
            for item in payments_list:
                name = item.get('name', '')
                amount = item.get('amount', 0)
                if name.startswith('Предоплата'):
                    prepayment_sum += amount
            return prepayment_sum

        if len(self.df_input) > 0:
            df = self.df_input.copy()
            df = df[df['returned'] == False]
            df['createDate'] = pd.to_datetime(
                df['createDate']).dt.date
            df['place'] = df['createTerminalSalePlace'].apply(
                lambda x: x.get('title'))

            # df['prepayment'] = df['payments'].apply(prepayment_sum)
            # df['frontTotalPrice'] = df['frontTotalPrice'] - df['prepayment']

            df['frontTotalPrice'] = df['payments'].apply(fiscal_sum)

            df = df.groupby(['place', 'createDate']
                            )['frontTotalPrice'].agg(['sum', 'mean', 'count']
                                                     ).reset_index()
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

    def write_xlsx_data(self):
        # self.load_data()
        display(self.df_input)
        self.write_xlsx(self.df_input)


class OrderOneDayReport(OrderReport):

    def load_data(self):

        qr = QROrderOneDayData(servers_data=self.servers_data,
                               module_settings=self.module_settings,
                               nubmer_of_months=self.nubmer_of_months,
                               days_time_step=self.days_time_step,
                               one_day_date=self.one_day_date)

        self.df_input = qr.get_data()


class OrderOneDaySalePlaceReport(OrderReport):

    def get_sale_place_id(self) -> str:
        sp_report = SalePlaceReport(servers_data=self.servers_data)
        sp_dict = sp_report.get_dict()
        return sp_dict.get(self.sale_place, 'None')

    def load_data(self):

        sale_place_id = self.get_sale_place_id()
        # print(f'sp {self.sale_place} {sale_place_id}')
        qr = QROrderOneDaySalePlaceData(servers_data=self.servers_data,
                                        module_settings=self.module_settings,
                                        nubmer_of_months=self.nubmer_of_months,
                                        days_time_step=self.days_time_step,
                                        one_day_date=self.one_day_date,
                                        sale_place_id=sale_place_id)

        self.df_input = qr.get_data()


class SalePlaceReport(OrderReport):

    module_settings = {
        'module_name': 'warehouse.nomenclature.sale_place',
        'module_date_field': '',
        'module_fields': ['title', '_id']
    }

    def load_data(self):

        self.df_input = QRData(servers_data=self.servers_data,
                               module_settings=self.module_settings).get_data()

    def get_dict(self):
        self.load_data()
        places = {}
        for (_, row) in self.df_input.iterrows():
            places[row['title']] = row['_id']
        self.places = places
        return self.places

    def proc_data(self):
        self.df = self.df_input


if __name__ == '__main__':

    # df_front_orders = OrderReport(servers_data=SERVERS,
    #                               nubmer_of_months=NUMBER_OF_MONTHS,
    #                               days_time_step=DAYS_STEP).get_report()
    # df_front_orders

    # df_sale_place = SalePlaceReport(servers_data=SERVERS).get_dict()
    # df_sale_place

    # one_day_date = (2021, 9, 11)

    # report = OrderOneDayReport(servers_data=SERVERS,
    #                            nubmer_of_months=NUMBER_OF_MONTHS,
    #                            days_time_step=DAYS_STEP,
    #                            one_day_date=one_day_date)
    # df_front_orders = report.get_report()

    # df_front_orders = QRDataOneDay(servers_data=SERVERS,
    #                                        nubmer_of_months=NUMBER_OF_MONTHS,
    #                                        days_time_step=DAYS_STEP,
    #                                        year=year, month=month, day=day).get_report()
    # df_front_orders

    one_day_date = (2021, 9, 11)
    sale_place = 'Место реализации 1'
    report = OrderOneDaySalePlaceReport(servers_data=SERVERS,
                                        nubmer_of_months=NUMBER_OF_MONTHS,
                                        days_time_step=DAYS_STEP,
                                        one_day_date=one_day_date,
                                        sale_place=sale_place)
    df_front_orders = report.get_report()
    report.write_xlsx_data()
    df_front_orders
