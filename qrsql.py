from datetime import date, timedelta

from mssql import SQLConnection
from quickresto import OrderDayReport
from setup import N_DAYS, QR_SERVERS_GROUPS, SQL_SERVER


class QrSql():

    def __init__(self,
                 db: SQLConnection,
                 n_days: int,
                 qr_servers: list,
                 order_table_name: str):
        self.n_days = n_days
        self.db = db
        self.qr_servers = qr_servers
        self.order_table_name = order_table_name

    def get_dates(self, reload: bool = False):
        today = date.today()
        dates = [today - timedelta(days=n)
                 for n in range(1, self.n_days+1)]
        if not reload:
            remote_dates = self.db.get_dates(table=self.order_table_name)
            dates = [x for x in dates if x not in remote_dates]
        return dates

    def get_orders(self, reload: bool = False):
        self.db.create_table(table=self.order_table_name)
        dates = self.get_dates(reload=reload)
        for day in dates:
            print(f'[{self.order_table_name}][{day}]', end=' ')
            report = OrderDayReport(servers_data=self.qr_servers, day=day)
            df = report.get_report()
            if len(df):
                self.db.update_df(table=self.order_table_name, df=df)


def main():
    db = SQLConnection(**SQL_SERVER)
    for group in QR_SERVERS_GROUPS:
        # db.delete_all_rows(group['order_table_name'])
        qs = QrSql(db=db, n_days=N_DAYS, **group)
        qs.get_orders(reload=False)


if __name__ == '__main__':
    main()