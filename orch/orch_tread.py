from concurrent.futures import ThreadPoolExecutor
import threading
from queue import Queue
from loguru import logger

from orch.orch import Orch


class OrchTread(Orch):

    def run(self) -> list:
        worker_show_config = self.workers['show_config'].run
        worker_servers_tasks = self.workers['servers_tasks'].run
        worker_show_servers_tasks = self.workers['show_servers_tasks'].run
        worker_order = self.workers['order'].run
        worker_order_aggregate = self.workers['order_aggregate'].run
        worker_store = self.workers['store'].run

        worker_show_config()
        servers_tasks = worker_servers_tasks()
        worker_show_servers_tasks(servers_tasks)

        queue_orders = Queue()
        queue_agg_orders = Queue()
        END_OF_QUEUE = object()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

            exctr_order_aggregate = executor.submit(
                worker_order_aggregate,
                queue_in=queue_orders,
                queue_out=queue_agg_orders,
                end_of_queue=END_OF_QUEUE)
            
            exctr_store = executor.submit(worker_store,
                                          queue_in=queue_agg_orders,
                                          end_of_queue=END_OF_QUEUE)

            exctrs_order = [executor.submit(worker_order, task, queue_orders)
                            for task in servers_tasks]
            
            results = [o.result() for o in exctrs_order]
            logger.debug(f'worker_order result: {results}')
            logger.debug(f'active_count {threading.active_count()}')
            queue_orders.put(END_OF_QUEUE)
            result = exctr_order_aggregate.result()
            logger.debug(f'worker_order_aggregate result: {result}')
            queue_agg_orders.put(END_OF_QUEUE)
            result = exctr_store.result()
            logger.debug(f'worker_store result: {result}')
            # logger.debug(f'results order_exs len {len(results)} {results}')
        logger.debug(f'Active threads count {threading.active_count()}')

        # logger.debug(f'OrchTread exit')
