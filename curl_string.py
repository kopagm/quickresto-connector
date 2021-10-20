from datetime import date, timedelta
import subprocess
from setup import QR_SERVERS_GROUPS

# Setup
day = date(2021, 10, 16)
server_name = 'ng398'


def convert_to_javatime(d) -> int:
    timestamp = int((d - date(1970, 1, 1)) / timedelta(seconds=1)) * 1000
    return timestamp


def main():

    since = convert_to_javatime(day)
    till = convert_to_javatime(day + timedelta(days=1))

    filter = {"filters":
              [{"field": "localCreateDate",
                "operation": "range",
                "value": {"since": since, "till": till}}
               ]}

    qr_server = [b for a in QR_SERVERS_GROUPS for b in a['qr_servers']
                 if b['server_name'] == server_name][0]

    filter = str(filter).replace(' ', '').replace("'", '"')
    curl_string = (
        f'curl -X GET '
        f'-u {qr_server["user"]}:{qr_server["password"]} '
        f'-H Content-Type:application/json '
        f'-d {filter} '
        f'https://{qr_server["server_name"]}.quickresto.ru/'
        f'platform/online/api/list?moduleName=front.orders')

    result = subprocess.run(curl_string.split(),
                            capture_output=True)
    print(f'[curl string]:\n{curl_string}\n')
    # print(f'[cur args]:\n{result.args}\n')
    # print(f'[out]:\n{result.stdout[:500]}')
    print(f'[out]:\n{result.stdout[:500].decode("utf-8")}')


if __name__ == '__main__':
    main()
