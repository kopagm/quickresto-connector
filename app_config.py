from mssql import SQLConnection
from my_setup import QR_SERVERS_GROUPS, SQL_SERVER, N_DAYS
from orch.orch_tread import OrchTread
from worker.order import Order
from worker.order_aggregate import OrderAggregate
from worker.order_store import OrderStore
from worker.servers_tasks import ServersTasks
from worker.show_config import ShowConfig
from worker.show_servers_tasks import ShowServersTasks

n_days = N_DAYS
# n_days = 5

db_conf = {"connection": SQLConnection,
           "sql_server": SQL_SERVER}
db = SQLConnection(**SQL_SERVER)

config = {'servers_groups': QR_SERVERS_GROUPS,
          'n_days': n_days,
          'reload': False,
          'db': db,
          'db_conf': db_conf
          }

max_workers = 10

workers = {'show_config': ShowConfig(**config),
           'servers_tasks': ServersTasks(**config),
           'show_servers_tasks': ShowServersTasks(**config),
           'order': Order(**config),
           'order_aggregate': OrderAggregate(**config),
           'store': OrderStore(**config)}

# treadPool
orch = OrchTread(workers=workers, max_workers=max_workers)
