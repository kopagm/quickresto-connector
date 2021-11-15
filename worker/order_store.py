from multiprocessing import JoinableQueue

from worker.worker import Worker


class OrderStore(Worker):

    def __init__(self, servers_groups: list, db_conf, *args, **kwargs):
        self.servers_groups = servers_groups
        self.db_conf = db_conf
        # self.db = db_conf['connection'](**db_conf['sql_server'])
        # print(db_conf)


    def store(self, item, db):
        order_table_name = item['order_table_name']
        df = item['df']
        if len(df):
            db.update_df(table=order_table_name, df=df)

    def run(self, queue_in: JoinableQueue) -> None:
        db = self.db_conf['connection'](**self.db_conf['sql_server'])
        while True:
            item = queue_in.get()
            self.store(item, db)
            queue_in.task_done()
