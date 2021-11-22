from loguru import logger

from mssql import SQLConnection
from orch.orch_tread import OrchTread
from setup import N_DAYS, QR_SERVERS_GROUPS, RELOAD, SQL_SERVER
from worker.order import Order
from worker.order_aggregate import OrderAggregate
from worker.order_store import OrderStore
from worker.servers_tasks import ServersTasks
from worker.show_config import ShowConfig
from worker.show_servers_tasks import ShowServersTasks

logger.add('log_app.log', rotation='0.2 MB',
           retention=1, enqueue=True, diagnose=True)

n_days = N_DAYS
# n_days = 5

db_conf = {"connection": SQLConnection,
           "sql_server": SQL_SERVER}
db = SQLConnection(**SQL_SERVER)
# db = ''

config = {'servers_groups': QR_SERVERS_GROUPS,
          'n_days': n_days,
          'reload': RELOAD,
          'db': db,
          #   'db_conf': db_conf
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
