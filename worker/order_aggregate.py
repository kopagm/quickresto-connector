from multiprocessing import JoinableQueue

import pandas as pd

from worker.worker import Worker


class OrderAggregate(Worker):

    def __init__(self, servers_groups: list, *args, **kwargs):
        self.servers_groups = servers_groups

    def fiscal_sum(self, payments_list: list) -> float:
        """Sum of order fiscal operations"""

        fiscal_sum = 0
        for item in payments_list:
            operationType = item.get('operationType', '')
            amount = item.get('amount', 0)
            if operationType == 'fiscal':
                fiscal_sum += amount
        return fiscal_sum

    def proc_data(self, df_input: pd.DataFrame) -> pd.DataFrame:
        """Data preprocessing"""

        if len(df_input):
            df = df_input.copy()
            df = df[df['returned'] == False]
            df['localCreateDate'] = pd.to_datetime(
                df['localCreateDate']).dt.date
            df['place'] = df['createTerminalSalePlace'].apply(
                lambda x: x.get('title'))

            df['totalPrice'] = df['payments'].apply(self.fiscal_sum)

            df = df.groupby(['place', 'localCreateDate', 'server_name']
                            )['totalPrice'].agg(['sum', 'mean', 'count']
                                                ).reset_index()
            df = df.rename(columns={'place': 'Place',
                                    'localCreateDate': 'Date',
                                    'sum': 'Revenue',
                                    'count': 'NumberOfOrders',
                                    'mean': 'AverageOrder',
                                    'server_name': 'ServerName'
                                    })
        else:
            df = pd.DataFrame([])
        return df

    def run(self, queue_in: JoinableQueue, queue_out: JoinableQueue) -> None:
        while True:
            item = queue_in.get()
            item['df'] = self.proc_data(item['df'])
            if len(item['df']):
                queue_out.put(item)
            queue_in.task_done()
