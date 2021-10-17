from datetime import date, timedelta

from mssql import SQLConnection
from quickresto import OrderDayReport
from setup import N_DAYS, QR_SERVERS, SQL_SERVER


class QrSql():

    def __init__(self,
                 db: SQLConnection,
                 n_days: int,
                 qr_servers: list):
        self.n_days = n_days
        self.db = db

    def get_dates(self, reload: bool = False):
        today = date.today()
        dates = [today - timedelta(days=n)
                 for n in range(1, self.n_days+1)]
        if not reload:
            remote_dates = self.db.get_dates()
            dates = [x for x in dates if x not in remote_dates]
        return dates

    def get_orders(self, reload: bool = False):
        dates = self.get_dates(reload=reload)
        for day in dates:
            report = OrderDayReport(servers_data=QR_SERVERS, day=day)
            df = report.get_report()
            if len(df):
                self.db.update_df(df)


if __name__ == '__main__':

    db = SQLConnection(**SQL_SERVER)
    # cnxn.delete_all_rows()
    # cnxn.create_table()

    qs = QrSql(db=db, n_days=N_DAYS, qr_servers=QR_SERVERS)
    qs.get_orders(reload=False)
