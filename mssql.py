from typing import Tuple
import pyodbc
import pandas as pd


class SQLConnection():

    def __init__(self, server: str, database: str, driver: str,
                 username: str, password: str):
        self.server = server
        self.database = database
        self.driver = driver
        self.username = username
        self.password = password

        self.table = 'orderinfo'
        self.key_fields = ['Place', 'Date']

        self.cnxn = self.get_connection()

    def get_connection(self):
        cnxn = pyodbc.connect(
            server=self.server,
            database=self.database,
            user=self.username,
            # tds_version='7.4',
            password=self.password,
            port=1433,
            driver=self.driver
        )
        # cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
        #                       server+';DATABASE='+database+';UID='+username+';PWD=' + password)

        return cnxn

    def get_dates(self, col: str = 'Date') -> list:
        cursor = self.cnxn.cursor()
        rows = cursor.execute(f"select {col} from {self.table}").fetchall()
        print(rows)
        cursor.close()
        rows = list(map(lambda x: x[0], rows))
        return rows

    def drop_table(self):
        # '''Drop table'''
        cursor = self.cnxn.cursor()

        qs = f'''IF OBJECT_ID('dbo.{self.table}', 'U') IS NOT NULL
                    DROP TABLE orderinfo'''
        cursor.execute(qs)
        cursor.commit()
        cursor.close()

    def create_table(self):
        # '''Insert DataFrame in table'''
        cursor = self.cnxn.cursor()

        qs = f'''CREATE TABLE {self.table}
                    (Place [NVARCHAR](150) NOT NULL,
                    Date [date] NOT NULL,
                    Revenue [money] NOT NULL,
                    NumberOfOrders [int] NOT NULL,
                    AverageOrder [float] NOT NULL,
                    primary key (Place, Date));'''

        cursor.execute(qs)
        cursor.commit()
        cursor.close()

    def get_querydata_insert(self, ser: pd.Series) -> Tuple[str, Tuple]:
        '''Insert DataFrame in table'''
        cols = ser.index
        names = ', '.join(cols)
        wildcards = ', '.join(list("?"*len(cols)))
        qs = f'''INSERT INTO {self.table}
                ({names})
                values({wildcards})'''
        values = tuple(ser.values)
        return (qs, values)

    def get_querydata_update(self, ser: pd.Series) -> Tuple[str, Tuple]:
        '''Insert DataFrame in table'''
        def mapper(x): return f'{x}=?'

        update_col = [x for x in ser.index if x not in self.key_fields]
        set_str = ', '.join(map(mapper, update_col))
        where_str = ' AND '.join(map(mapper, self.key_fields))

        # "update users set last_logon=? where user_id=?"
        qs = f'''UPDATE {self.table}
                SET {set_str}
                WHERE {where_str}'''
        # print(qs)
        values = tuple(ser[update_col]) + tuple(ser[self.key_fields])
        return (qs, values)


    def incert_df(self, df: pd.DataFrame):
        '''Insert DataFrame in table'''
        cursor = self.cnxn.cursor()

        for index, row in df.iterrows():
            qs, values = self.get_querydata_insert(row)
            cursor.execute(qs, values)

        cursor.commit()
        cursor.close()

    def update_df(self, df: pd.DataFrame):
        '''Update table from DataFrame'''

        cursor = self.cnxn.cursor()

        for index, row in df.iterrows():
            qs, values = self.get_querydata_update(row)
            count = cursor.execute(qs, values).rowcount
            if count == 0:
                qs, values = self.get_querydata_insert(row)
                cursor.execute(qs, values)

        cursor.commit()
        cursor.close()


if __name__ == '__main__':
    server = {
        'server': 'localhost',
        'database': 'qr',
        'driver': 'FreeTDS',
        'username': 'sa',
        'password': '1234Sql!'}
    con = SQLConnection(**server)
    # rows = con.get_dates()
    # print(rows)
    df = pd.read_excel("out.xlsx", sheet_name='1')
    # con.incert_df(df)
    # rows = con.get_dates()
    # print(rows)
    # con.drop_table()
    # con.create_table()
    # con.incert_df(df)
    con.update_df(df)
    print(con.get_dates())
