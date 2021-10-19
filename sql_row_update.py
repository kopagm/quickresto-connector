import pandas as pd

from mssql import SQLConnection
from setup import SQL_SERVER


table = 'order_xxx'
data = {
    'Place': ['place'],
    'Date': ['2021-10-14'],
    'Revenue': [1.0000],
    'NumberOfOrders': [1],
    'AverageOrder': [1.0],
    'ServerName': ['ng398']
}


if __name__ == '__main__':
    db = SQLConnection(**SQL_SERVER)
    df = pd.DataFrame.from_dict(data)
    if len(df):
        db.update_df(table=table, df=df)
