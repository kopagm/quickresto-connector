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
        self.parts = 10
        self.parts_max = 320
        self.parts_step = 30
        self.url = (
            f"https://{self.server_data['server_name']}."
            f"quickresto.ru/platform/online/api/list?"
            f"moduleName={self.module_settings['module_name']}"
        )
        self.session = self.get_session()

    def get_session(self):
        session = requests.Session()
        session.auth = (self.server_data["user"], self.server_data["password"])
        session.headers.update({"Content-Type": "application/json"})
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

    def get_api(self, filter: dict = {}) -> dict:
        response = self.session.get(self.url, json=filter)
        result = self.proc_response(response=response)
        return result

    def convert_to_javatime(self, d: datetime.date) -> int:
        timestamp = int((d - date(1970, 1, 1)) / timedelta(seconds=1)) * 1000
        return timestamp

    def get_filter(self, since: int, till: int) -> dict:
        filter = {
            "filters": [
                {
                    "field": self.module_settings["module_date_field"],
                    "operation": "range",
                    "value": {"since": since, "till": till},
                }
            ]
        }
        return filter

    def get_parts(self, day: date) -> pd.DataFrame:
        logger.info(
            f"[Server: {self.server_data['server_name']}, Day: {day}] "
            f"Try with {self.parts} parts..."
        )
        df = pd.DataFrame([])
        since = self.convert_to_javatime(day)
        till = self.convert_to_javatime(day + timedelta(days=1))
        step = (till - since) // self.parts
        p_since, p_till = since, since + step
        while p_till <= till:
            if p_till > till:
                p_till = till
            filter = self.get_filter(since=p_since, till=p_till)
            response_dict = self.get_api(filter=filter)["json"]
            df_part = pd.DataFrame.from_dict(response_dict)
            df = df.append(df_part, ignore_index=True)
            p_since, p_till = p_till + 1, p_till + step
        return df

    def get_data(self, day: date) -> pd.DataFrame:
        result = None
        while result is None:
            try:
                result = self.get_parts(day=day)
            except (ConnectionError, ValueError) as error:
                logger.warning(error)
                logger.warning(type(error))
                if self.parts + self.parts_step <= self.parts_max:
                    self.parts += self.parts_step
                else:
                    logger.warning(
                        f"[Server: {self.server_data['server_name']}, "
                        f"Day: {day}] "
                        f"Fails with {self.parts} parts"
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
