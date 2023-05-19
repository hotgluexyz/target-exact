from target_hotglue.client import HotglueSink
import json
from datetime import datetime
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
from target_exact.auth import ExactAuthenticator
import backoff
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


class ExactSink(HotglueSink):

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        """Initialize target sink."""
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)

    @property
    def exact_environment(self) -> str:
        refresh_token = self.config["refresh_token"].split(".")[0]
        if "NL" in refresh_token:
            return "nl"
        elif "UK" in refresh_token:
            return "co.uk"
        else:
            return "com"

    @property
    def base_url(self) -> str:
        current_division = self.config.get("current_division")
        base_url = f"https://start.exactonline.{self.exact_environment}/api/v1/{current_division}"
        return base_url
    
    @property
    def authenticator(self):
        return ExactAuthenticator(
            self._target, f"https://start.exactonline.{self.exact_environment}/api/oauth2/token"
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers.update(self.authenticator.auth_headers or {})
        return headers

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params=None, request_data=None, headers=None
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers = self.http_headers

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )
        self.validate_response(response)
        return response


    def validate_input(self, record: dict):
        return self.unified_schema(**record).dict()

    def parse_json(self, input):
        # if it's a string, use json.loads, else return whatever it is
        if isinstance(input, str):
            return json.loads(input)
        return input

    def convert_datetime(self, date: datetime):
        # convert datetime.datetime into str
        if isinstance(date, datetime):
            # This is the format -> "2022-08-15T19:16:35Z"
            return date.strftime("%Y-%m-%dT%H:%M:%SZ")
        return date