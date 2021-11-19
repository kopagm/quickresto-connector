from datetime import date, datetime, timedelta

from loguru import logger

from mssql import SQLConnection
from quickresto import OrderDayReport
from setup import N_DAYS, QR_SERVERS_GROUPS, SQL_SERVER


class QrSql():

    def __init__(self, db: SQLConnection):
        self.db = db

    def get_dates(self, server_name: str, n_days: int,
                  order_table_name: str, reload: bool):
        today = date.today()
        dates = [today - timedelta(days=n)
                 for n in range(1, n_days+1)]
        if not reload:
            remote_dates = self.db.get_server_dates(
                table=order_table_name,
                server_name=server_name)
            dates = [x for x in dates if x not in remote_dates]
        return dates

    def get_orders(self, n_days: int, qr_servers: list,
                   order_table_name: str, reload: bool = False):
        self.db.create_table(table=order_table_name)
        report = OrderDayReport()

        for server in qr_servers:
            dates = self.get_dates(server_name=server['server_name'],
                                   n_days=n_days,
                                   order_table_name=order_table_name,
                                   reload=reload)
            for day in dates:
                log_str = (
                    # f'[{datetime.now().strftime("%m.%d.%Y %H:%M:%S")}]'
                    f'[{order_table_name}]'
                    f'[{server["server_name"]}][{day}]')
                # print(log_str, end=' ')
                logger.info(log_str)
                # sys.stdout.flush()
                df = report.get_report(server_data=server, day=day)
                if len(df):
                    self.db.update_df(table=order_table_name, df=df)


def main():
    logger.add('log_qrsql.log', rotation="5 MB")
    db = SQLConnection(**SQL_SERVER)
    qs = QrSql(db=db)
    for group in QR_SERVERS_GROUPS:
        # db.drop_table(group['order_table_name'])
        # db.delete_all_rows(group['order_table_name'])
        qs.get_orders(reload=False, n_days=N_DAYS, **group)
        # qs.get_orders(reload=True, n_days=N_DAYS, **group)


if __name__ == '__main__':
    main()
