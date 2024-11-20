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
)

from target_hotglue.target import TargetHotglue
from typing import Callable, Dict, List, Optional, Tuple, Type, Union
from pathlib import Path, PurePath



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


    SINK_TYPES = [BuyOrdersSink, UpdateInventory, ItemsSink, PurchaseInvoicesSink, SuppliersSink, PurchaseEntriesSink, SalesOrdersSink, ShopOrdersSink,]
    MAX_PARALLELISM = 10
    name = "target-exact"


if __name__ == "__main__":
    TargetExact.cli()

