from typing import Tuple
import pyodbc
import pandas as pd

from setup import SQL_SERVER


class SQLConnection():

    def __init__(self, server: str, database: str, driver: str,
                 username: str, password: str):
        self.server = server
        self.database = database
        self.driver = driver
        self.username = username
        self.password = password

        self.key_fields = ['Place', 'Date', 'ServerName']

        self.cnxn = self.get_connection()

    def get_connection(self):
        cnxn = pyodbc.connect(
            server=self.server,
            database=self.database,
            user=self.username,
            # tds_version='7.4',
            password=self.password,
            port=1433,
            driver=self.driver)
        return cnxn

    def get_dates(self, table: str, col: str = 'Date') -> list:
        cursor = self.cnxn.cursor()
        rows = cursor.execute(f"select {col} from {table}").fetchall()
        # print(rows)
        cursor.close()
        rows = list(map(lambda x: x[0], rows))
        return rows

    def get_server_dates(self, table: str, server_name: str) -> list:
        cursor = self.cnxn.cursor()
        qs = f"select Date from {table} WHERE ServerName = '{server_name}'"
        rows = cursor.execute(qs).fetchall()
        # print(rows)
        cursor.close()
        rows = list(map(lambda x: x[0], rows))
        return rows

    def drop_table(self, table: str):
        # '''Drop table'''
        cursor = self.cnxn.cursor()

        qs = f'''IF OBJECT_ID('{table}', 'U') IS NOT NULL
                    DROP TABLE {table}'''
        cursor.execute(qs)
        cursor.commit()
        cursor.close()

    def create_table(self, table: str):
        # '''Insert DataFrame in table'''
        cursor = self.cnxn.cursor()

        qs = f'''   if object_id('{table}') is null
                    CREATE TABLE {table}
                        (Place [NVARCHAR](150) NOT NULL,
                        Date [date] NOT NULL,
                        Revenue [money] NOT NULL,
                        NumberOfOrders [int] NOT NULL,
                        AverageOrder [float] NOT NULL,
                        ServerName [NVARCHAR](50) NOT NULL,
                        primary key (Place, Date, ServerName));'''

        cursor.execute(qs)
        cursor.commit()
        cursor.close()

    def get_querydata_insert(self, table: str,
                             ser: pd.Series) -> Tuple[str, Tuple]:
        '''Insert DataFrame in table'''
        cols = ser.index
        names = ', '.join(cols)
        wildcards = ', '.join(list("?"*len(cols)))
        # "insert into t(name, id) values (?, ?)"
        qs = f'''INSERT INTO {table}
                    ({names})
                    values({wildcards})'''
        values = tuple(ser.values)
        return (qs, values)

    def get_querydata_update(self, table: str,
                             ser: pd.Series) -> Tuple[str, Tuple]:
        '''Insert DataFrame in table'''
        def mapper(x): return f'{x}=?'

        update_col = [x for x in ser.index if x not in self.key_fields]
        set_str = ', '.join(map(mapper, update_col))
        where_str = ' AND '.join(map(mapper, self.key_fields))

        # "update users set last_logon=? where user_id=?"
        qs = f'''UPDATE {table}
                    SET {set_str}
                    WHERE {where_str}'''
        values = tuple(ser[update_col]) + tuple(ser[self.key_fields])
        return (qs, values)

    def update_df(self, table: str, df: pd.DataFrame):
        '''Update table from DataFrame'''

        cursor = self.cnxn.cursor()

        for index, row in df.iterrows():
            # update
            qs, values = self.get_querydata_update(table, row)
            count = cursor.execute(qs, values).rowcount
            # insert if not update
            if count == 0:
                qs, values = self.get_querydata_insert(table, row)
                cursor.execute(qs, values)

        cursor.commit()
        cursor.close()

    def delete_all_rows(self, table: str):
        '''Delete rows from table'''

        cursor = self.cnxn.cursor()

        qs = f'''DELETE FROM {table}'''
        # "delete from users"
        cursor.execute(qs)

        cursor.commit()
        cursor.close()

    def delete_rows(self, table: str, field: str, start_v: str, end_v: str):
        '''Delete rows from table'''

        cursor = self.cnxn.cursor()

        qs = f'''DELETE FROM {table}
                    WHERE {field} BETWEEN ? AND ?'''
        values = (start_v, end_v)
        # "delete from users where user_id=1"
        cursor.execute(qs, values)

        cursor.commit()
        cursor.close()


if __name__ == '__main__':
    cnxn = SQLConnection(**SQL_SERVER)
    # df = pd.read_excel("out.xlsx", sheet_name='1')
    # cnxn.incert_df(df)
    # cnxn.drop_table()
    # cnxn.create_table()
    # cnxn.update_df(df)
    # con.delete_rows('Date', '2021-09-12', '2021-09-15')
    # print(cnxn.get_dates())
