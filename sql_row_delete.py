from mssql import SQLConnection
from setup import SQL_SERVER

table = 'order_xxx'
date_to_del = '2021-10-16'
server_name = 'ng398'

if __name__ == '__main__':
    db = SQLConnection(**SQL_SERVER)
    db.delete_rows(table=table, eq_col1=date_to_del, eq_col2=server_name)
