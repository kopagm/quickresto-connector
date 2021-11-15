from queue import Queue

from api.qr_order_data import QROrderData

from worker.worker import Worker


class Order(Worker):

    def __init__(self, servers_groups: list, *args, **kwargs):
        self.servers_groups = servers_groups

    def run(self, task: dict, queue: Queue):
        server_data = task['server']
        order_table_name = task['order_table_name']
        order = QROrderData(server_data=server_data)
        for day in task['dates']:
            df = order.get(day=day)
            if len(df):
                df['server_name'] = server_data.get('server_name')
                day_data = {'order_table_name': order_table_name,
                            'df': df}
                queue.put(day_data)
