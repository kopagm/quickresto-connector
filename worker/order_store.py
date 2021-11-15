from multiprocessing import JoinableQueue

from worker.worker import Worker


class OrderStore(Worker):

    def __init__(self, servers_groups: list, db, *args, **kwargs):
        self.servers_groups = servers_groups
        self.db = db

    def store(self, item):
        order_table_name = item['order_table_name']
        df = item['df']
        if len(df):
            self.db.update_df(table=order_table_name, df=df)

    def run(self, queue_in: JoinableQueue) -> None:
        while True:
            item = queue_in.get()
            self.store(item)
            queue_in.task_done()
