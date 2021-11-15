from datetime import date, datetime

import pandas as pd

from app_config import db, orch


class App():
    def __init__(self, orch, db):
        self.orch = orch
        self.db = db

    def fetch(self):
        # db.delete()
        top = self.orch.run()


if __name__ == '__main__':
    app = App(orch=orch, db=db)
    start_time = datetime.now()
    app.fetch()
    time_delta = datetime.now() - start_time
    print(f'{"-"*40}\nTotal minutes: {time_delta.total_seconds()/60.:.2f}')
