class Orch():

    def __init__(self, workers: dict, max_workers: int = 1,
                 *args, **kwargs) -> None:
        self.workers = workers
        self.max_workers = max_workers

    def run(self):
        pass
