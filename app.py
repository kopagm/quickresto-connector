from datetime import date, datetime

import pandas as pd

from app_config import db, orch
from loguru import logger


class App():
    def __init__(self, orch, db):
        self.orch = orch
        self.db = db

    def fetch(self):
        # db.delete()
        top = self.orch.run()

@logger.catch
def main():
    logger.info(f'{"-"*5} Start {"-"*10}')
    app = App(orch=orch, db=db)
    start_time = datetime.now()
    app.fetch()
    time_delta = datetime.now() - start_time
    logger.info(f'{"-"*5} End {"-"*12}\nTotal minutes: {time_delta.total_seconds()/60.:.2f}')

if __name__ == '__main__':
    main()