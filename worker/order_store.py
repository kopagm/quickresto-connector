import sys
from datetime import datetime
from multiprocessing import JoinableQueue

from worker.worker import Worker


class OrderStore(Worker):

    def __init__(self, db_conf: dict, *args, **kwargs):
        self.db_conf = db_conf

    def store(self, item, db):
        order_table_name = item['order_table_name']
        df = item['df']
        if len(df):
            count = db.update_df(table=order_table_name, df=df)
            print(f'[{datetime.now().strftime("%m.%d.%Y %H:%M:%S")}] '
                  f'[{df.loc[0, "ServerName"]}] [{df.loc[0, "Date"]}] '
                  f'[store {count} rows]')
        sys.stdout.flush()

    def run(self, queue_in: JoinableQueue) -> None:
        db = self.db_conf['connection'](**self.db_conf['sql_server'])

        while True:
            item = queue_in.get()
            self.store(item, db)
            queue_in.task_done()
