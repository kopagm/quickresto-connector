from datetime import date

from quickresto import OrderDayReport
from setup import N_DAYS, QR_SERVERS_GROUPS, SQL_SERVER

day = date(2021, 10, 16)
server_name = 'ng398'


def main():

    qr_server = [b for a in QR_SERVERS_GROUPS for b in a['qr_servers']
                 if b['server_name'] == server_name][0]

    report = OrderDayReport()
    path = f'{day}-{server_name}.xlsx'
    report.get_xlsx_data(server_data=qr_server, day=day, path=path)


if __name__ == '__main__':
    # pass
    main()
