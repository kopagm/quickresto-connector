from concurrent.futures import ThreadPoolExecutor
from multiprocessing import JoinableQueue, Process

from orch.orch import Orch


class OrchTread(Orch):

    def run(self) -> list:
        show_config = self.workers['show_config'].run
        servers_tasks = self.workers['servers_tasks'].run
        show_servers_tasks = self.workers['show_servers_tasks'].run
        order = self.workers['order'].run
        order_aggregate = self.workers['order_aggregate'].run
        store = self.workers['store'].run

        show_config()
        servers_tasks = servers_tasks()
        # print(f'orc server_tasks {servers_tasks}')
        show_servers_tasks(servers_tasks)

        queue_orders = JoinableQueue()
        queue_agg_orders = JoinableQueue()

        # proc_aggregate = Process(target=order_aggregate, args=(queue_orders, queue_agg_orders))
        proc_aggregate = Process(target=order_aggregate,
                                 kwargs={'queue_in': queue_orders,
                                         'queue_out': queue_agg_orders},
                                 name='proc_aggregate')
        proc_store = Process(target=store,
                             kwargs={'queue_in': queue_agg_orders},
                             name='proc_store')

        proc_aggregate.start()
        proc_store.start()

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                tasks = [executor.submit(order, task, queue_orders)
                         for task in servers_tasks]
                tasks = [task.result() for task in tasks]
                # tasks = [task.result() for task in tasks]
        except Exception as ex:
            # print('inst', type(inst), inst.__traceback__.tb_frame , inst)
            raise ex
        finally:
            queue_orders.join()
            # print('join')
            proc_aggregate.terminate()
            queue_agg_orders.join()
            proc_store.terminate()
        # print(f'exit')
