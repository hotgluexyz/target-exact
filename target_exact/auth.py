import json
from datetime import datetime
from typing import Any, Dict, Optional
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.target_sdk.auth import OAuthAuthenticator

import logging
import requests
import backoff

INVALID_CREDENTIALS_OAUTH_ERRORS = {"invalid_grant", "invalid_client", "unauthorized_client"}
MALFORMED_REFRESH_TOKEN_DESCRIPTION = "Handle could not be extracted"


def is_invalid_credentials_response(response: requests.Response) -> bool:
    """Whether a failed token response means the connection's credentials are bad."""
    try:
        body = response.json()
    except ValueError:
        body = None
    error = body.get("error") if isinstance(body, dict) else None
    if not error:
        return response.status_code == 401
    description = body.get("error_description") or ""
    return error in INVALID_CREDENTIALS_OAUTH_ERRORS or (
        error == "invalid_request" and MALFORMED_REFRESH_TOKEN_DESCRIPTION in description
    )


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
        super().__init__(target, state, auth_endpoint)
        self.target_name: str = target.name
        self._config: Dict[str, Any] = target._config
        self._auth_headers: Dict[str, Any] = {}
        self._auth_params: Dict[str, Any] = {}
        self.logger: logging.Logger = target.logger
        self._auth_endpoint = auth_endpoint
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

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=3,
        giveup=lambda error: isinstance(error, InvalidCredentialsError),
    )
    def _update_access_token_locally(self) -> None:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        self.logger.info(f"Oauth request - endpoint: {self._auth_endpoint}")
        token_response = requests.post(
            self._auth_endpoint, data=self.oauth_request_body, headers=headers
        )

        try:
            if (
                token_response.json().get("error_description")
                == "Rate limit exceeded: access_token not expired"
            ):
                return None
        except (ValueError, AttributeError) as ex:
            error_class = (
                InvalidCredentialsError
                if is_invalid_credentials_response(token_response)
                else Exception
            )
            raise error_class(
                f"Failed converting response to a json, OAuth response: {token_response.text}"
            ) from ex

        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            self.state.update({"auth_error_response": token_response.json()})
            error_class = (
                InvalidCredentialsError
                if is_invalid_credentials_response(token_response)
                else RuntimeError
            )
            raise error_class(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            ) from ex
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

