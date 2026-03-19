import json
from datetime import datetime
from typing import Any, Dict, Optional
from hotglue_singer_sdk.target_sdk.auth import OAuthAuthenticator

import logging
import requests
import backoff


class ExactAuthenticator(OAuthAuthenticator):
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        target,
        state,
        auth_endpoint: Optional[str] = None,
    ) -> None:
        """Init authenticator.

        Args:
            stream: A stream for a RESTful endpoint.
        """
        self.target_name: str = target.name
        self._config: Dict[str, Any] = target._config
        self._auth_headers: Dict[str, Any] = {}
        self._auth_params: Dict[str, Any] = {}
        self.logger: logging.Logger = target.logger
        self._auth_endpoint = auth_endpoint
        self._config_file = target.config_file
        self._target = target
        self.state = state

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the hubspot API."""
        return {
            "refresh_token": self._config["refresh_token"],
            "grant_type": "refresh_token",
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"],
        }

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def update_access_token_locally(self) -> None:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        self.logger.info(f"Oauth request - endpoint: {self._auth_endpoint}, body: {self.oauth_request_body}")
        token_response = requests.post(
            self._auth_endpoint, data=self.oauth_request_body, headers=headers
        )

        try:
            if (
                token_response.json().get("error_description")
                == "Rate limit exceeded: access_token not expired"
            ):
                return None
        except:
            raise Exception(f"Failed converting response to a json, OAuth response: {token_response.text}")

        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            self.state.update({"auth_error_response": token_response.json()})
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        #Log the refresh_token
        self.logger.info(f"Latest refresh token: {token_json['refresh_token']}")
        
        self.access_token = token_json["access_token"]

        self._config["access_token"] = token_json["access_token"]
        self._config["refresh_token"] = token_json["refresh_token"]
        now = round(datetime.utcnow().timestamp())
        self._config["expires_in"] = int(token_json["expires_in"]) + now

        with open(self._target.config_file, "w") as outfile:
            json.dump(self._config, outfile, indent=4)

