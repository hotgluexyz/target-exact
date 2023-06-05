from target_hotglue.client import HotglueSink
import json
from datetime import datetime
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
from target_exact.auth import ExactAuthenticator
import backoff
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
import xmltodict

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
    
    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            res_json = xmltodict.parse(response.text)
            state = {"response": res_json["error"]["message"]["#text"]}
            self.update_state(state)
            print("ERROR:", state)
            raise RetriableAPIError(msg, res_json)
        elif 400 <= response.status_code < 500:
            res_json = xmltodict.parse(response.text)
            state = {"response": res_json["error"]["message"]["#text"]}
            try:
                msg = response.text
                self.update_state(state)
                print("ERROR:", state)
            except:
                msg = self.response_error_message(response)
            raise FatalAPIError(msg, res_json)
    
    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        if not self.latest_state:
            self.init_state()

        hash = self.build_record_hash(record)

        existing_state =  self.get_existing_state(hash)

        if existing_state:
            return self.update_state(existing_state, is_duplicate=True)

        state = {"hash": hash}

        id = None
        success = False
        state_updates = dict()

        try:
            id, success, state_updates = self.upsert_record(record, context)
        except Exception as e:
            self.logger.exception("Upsert record error")
            raise Exception(e)

        if success:
            self.logger.info(f"{self.name} created with id: {id}")

        state["success"] = success

        if id:
            state["id"] = id

        if state_updates and isinstance(state_updates, dict):
            state = dict(state, **state_updates)

        self.update_state(state)
    
    def request_api(self, http_method, endpoint=None, params={}, request_data=None, headers={}):
        """Request records from REST endpoint(s), returning response records."""
        print("REQUEST - endpoint:", endpoint, "request_body:", request_data)
        resp = self._request(http_method, endpoint, params, request_data, headers)
        return resp