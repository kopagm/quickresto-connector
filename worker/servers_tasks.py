from datetime import date, timedelta

from loguru import logger

from worker.worker import Worker


class ServersTasks(Worker):

    def __init__(self, servers_groups: list, n_days: int, reload: bool,
                #  db_conf: dict,
                 db,
                 *args, **kwargs):
        self.servers_groups = servers_groups
        self.n_days = n_days
        self.reload = reload
        # self.db_conf = db_conf
        self.db = db

    def get_dates(self, server_name: str,
                  order_table_name: str):
        # all n_days
        today = date.today()
        dates = [today - timedelta(days=n)
                 for n in range(1, self.n_days+1)]
        # chek or create table
        self.db.create_table(table=order_table_name)
        # filter days
        if not self.reload:
            remote_dates = self.db.get_server_dates(
                table=order_table_name,
                server_name=server_name)
            dates = [x for x in dates if x not in remote_dates]
        return dates

    def add_dates_mapper(self, server):
        server_name = server['server']['server_name']
        order_table_name = server['order_table_name']
        dates = self.get_dates(server_name=server_name,
                                order_table_name=order_table_name)
        server['dates'] = dates
        return server

    def db_init(self):
        return self.db_conf['connection'](**self.db_conf['sql_server'])

    @logger.catch
    def run(self):
        servers = [{'server': s,
                    'order_table_name': g['order_table_name']}
                   for g in self.servers_groups for s in g['qr_servers']]
        # if not self.reload:
            # self.db = self.db_init()
        servers_tasks = list(map(self.add_dates_mapper, servers))
        return servers_tasks
