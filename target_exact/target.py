"""Exact target class."""
from target_exact.sinks import (
    BuyOrdersSink,
    UpdateInventory,
    ItemsSink,
    PurchaseInvoicesSink,
    SuppliersSink,
    PurchaseEntriesSink,
    SalesOrdersSink,
    ShopOrdersSink,
    WarehouseTransfersSink
)

from hotglue_singer_sdk.target_sdk.target import TargetHotglue
from typing import List, Optional, Union
from pathlib import PurePath
from target_exact.auth import ExactAuthenticator



class TargetExact(TargetHotglue):
    """Sample target for Exact."""

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, parse_env_config, validate_config)


    SINK_TYPES = [BuyOrdersSink, UpdateInventory, ItemsSink, PurchaseInvoicesSink, SuppliersSink, PurchaseEntriesSink, SalesOrdersSink, ShopOrdersSink, WarehouseTransfersSink]
    MAX_PARALLELISM = 10
    name = "target-exact"

    @classmethod
    def access_token_support(cls, connector=None):
        """Return (authenticator_class, auth_endpoint). Use connector.config when connector (tap instance) is provided."""
        authenticator = ExactAuthenticator
        default_url = "https://start.exactonline.nl/api/oauth2/token"

        if connector is not None and getattr(connector, "config", None) is not None:
            oauth_url = connector.config.get("auth_url", connector.config.get("uri")) or "https://start.exactonline.nl/api/oauth2/token"
        else:
            oauth_url = default_url

        if "token" not in oauth_url:
            oauth_url = f"{oauth_url}/api/oauth2/token"
        if not oauth_url.endswith("/token"):
            oauth_url += "/token"

        return authenticator, oauth_url

if __name__ == "__main__":
    TargetExact.cli()

