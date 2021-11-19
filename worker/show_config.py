from loguru import logger

from worker.worker import Worker


class ShowConfig(Worker):

    def __init__(self, servers_groups: list, n_days: int, reload: bool,
                 *args, **kwargs):
        self.servers_groups = servers_groups
        self.n_days = n_days
        self.reload = reload

    def run(self):
        conf_str = []
        for group in self.servers_groups:
            for server in group['qr_servers']:
                conf_str.append((f'Group: {group["order_table_name"]}, '
                                 f'Server: {server["server_name"]}'))
        conf_str = '\n'.join(conf_str)
        log_str = (f'Config\n'
                   f'N_Days: {self.n_days}, reload: {self.reload}\n'
                   f'{conf_str}'
                #    f'{"-"*40}'
                   )
        logger.info(log_str)
