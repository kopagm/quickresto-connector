QR_SERVERS_GROUPS = [
    # region xxx1
    {'order_table_name': 'order_xxx',
     'qr_servers': [
         # server #1
         {'server_name': '',
          'user': '',
          'password': ''},
         # server #2...
     ]},
    # region xxx2...
]

SQL_SERVER = {
    'server': 'localhost',
    'database': 'qr',
    # 'driver': 'FreeTDS',
    'driver': 'ODBC Driver 17 for SQL Server',
    'username': '',
    'password': ''}

N_DAYS = 4

RELOAD = False
# RELOAD = True
