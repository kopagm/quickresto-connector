from worker.worker import Worker


class ShowServersTasks(Worker):

    def run(self, tasks):
        log_str = []
        for task in tasks:
            log_str.append((
                f'[Group]: {task["order_table_name"]}, '
                f'[Server]: {task["server"]["server_name"]}, '
                f'[Dates]: {list(map(str, task["dates"]))}'
            ))
        log_str = '\n'.join(log_str)
        log_str = f'[Tasks]\n{log_str}\n{"-"*40}'

        print(log_str)
