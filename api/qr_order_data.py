from datetime import date, datetime, timedelta
from json.decoder import JSONDecodeError

import pandas as pd
import requests
from loguru import logger
from requests.exceptions import ConnectionError
from requests.models import Response

from api.qr_order import QROrder


class QROrderData(QROrder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.limit = 1000
        self.url = (
            f"https://{self.server_data['server_name']}."
            f"quickresto.ru/platform/online/api/list?"
            f"moduleName={self.module_settings['module_name']}"
            f"&className={self.module_settings['class_name']}"
            if self.module_settings["class_name"]
            else ""
        )
        self.session = self.get_session()

    def get_session(self):
        session = requests.Session()
        session.auth = (self.server_data["user"], self.server_data["password"])
        session.headers.update(
            {"Content-Type": "application/json", "Connection": "keep-alive"}
        )
        return session

    def proc_response(self, response: Response) -> dict:
        code = response.status_code
        if code == 200:
            try:
                json = response.json()
            except JSONDecodeError:
                raise ValueError(code, "JSONDecodeError", response.text)
            else:
                result = {"code": response.status_code, "json": json}
        else:
            raise ValueError(code, response.text)
        return result

    def get_api_response(self, request_body: dict = {}) -> dict:
        response = self.session.get(
            self.url,
            json=request_body,
        )
        result = self.proc_response(response=response)
        return result

    def convert_to_javatime(self, d: datetime.date) -> int:
        timestamp = int((d - date(1970, 1, 1)) / timedelta(seconds=1)) * 1000
        return timestamp

    def get_request_body(
        self, since: int, till: int, limit: int = 1000, offset: int = 0
    ) -> dict:
        filter = {
            "filters": [
                {
                    "field": self.module_settings["module_date_field"],
                    "operation": "range",
                    "value": {"since": since, "till": till},
                }
            ],
            "limit": limit,
            "offset": offset,
        }
        return filter

    def get_day_data(self, day: date, limit: int) -> pd.DataFrame:
        logger.info(
            f"[Server: {self.server_data['server_name']}, Day: {day}] "
            f"Trying with limit {limit} ..."
        )
        df = pd.DataFrame([])
        since = self.convert_to_javatime(day)
        till = self.convert_to_javatime(day + timedelta(days=1))
        offset = 0
        responce_length = limit
        while responce_length == limit:
            request_body = self.get_request_body(
                since=since, till=till, limit=limit, offset=offset
            )
            response_dict = self.get_api_response(request_body=request_body)["json"]
            responce_length = len(response_dict)
            df_part = pd.DataFrame.from_dict(response_dict)
            logger.debug(
                f"[Server: {self.server_data['server_name']}, Day: {day}, Limit: {limit}, Offset: {offset}] "
                f"Recieved: {responce_length}"
            )
            df = df.append(df_part, ignore_index=True)
            offset += limit
        return df

    def get_data(self, day: date) -> pd.DataFrame:
        result = None
        limit = self.limit
        while result is None:
            try:
                result = self.get_day_data(day=day, limit=limit)
            except (ConnectionError, ValueError) as error:
                logger.warning(error)
                logger.warning(type(error))
                if limit > 1:
                    limit = limit / 10
                    if limit < 1:
                        limit = 1
                else:
                    logger.warning(
                        f"[Server: {self.server_data['server_name']}, "
                        f"Day: {day}] "
                        f"Fails with limit {limit}."
                    )
                    result = pd.DataFrame([])
        logger.info(
            f"[Server: {self.server_data['server_name']}, Day: {day}] "
            f"Loaded {len(result)} lines"
        )
        return result

    def select_df_colummns(self, df_input: pd.DataFrame) -> pd.DataFrame:
        df = df_input.copy()
        module_fields = self.module_settings["module_fields"]
        if len(module_fields) & len(df.columns):
            df = df[module_fields]
        return df

    @logger.catch
    def get(self, day: date) -> pd.DataFrame:
        df = self.get_data(day)
        # drop columns
        df = self.select_df_colummns(df)
        return df
