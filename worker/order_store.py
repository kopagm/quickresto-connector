from queue import Queue

from loguru import logger

from worker.worker import Worker


class OrderStore(Worker):

    def __init__(self,
                #  db_conf: dict,
                 db,
                 *args, **kwargs):
        # self.db_conf = db_conf
        self.db = db

    def store(self, item, db):
        order_table_name = item['order_table_name']
        df = item['df']
        if len(df):
            try:
                count = db.update_df(table=order_table_name, df=df)
                logger.info(#f'[{datetime.now().strftime("%m.%d.%Y %H:%M:%S")}] '
                    f'[Server: {df.loc[0, "ServerName"]}, '
                    f'Day: {df.loc[0, "Date"]}] '
                    f'stored {count} rows')
            except:
                logger.exception('store failed')

    @logger.catch
    def run(self, queue_in: Queue, end_of_queue) -> None:
        # db = self.db_conf['connection'](**self.db_conf['sql_server'])
        item = queue_in.get()
        while item is not end_of_queue:
            self.store(item, self.db)
            queue_in.task_done()
            item = queue_in.get()
        return 0