"""Exact target sink class, which handles writing streams."""


from target_exact.client import ExactSink
import ast
import xmltodict
import json

class BuyOrdersSink(ExactSink):
    """Qls target sink class."""

    name = "BuyOrders"
    endpoint = "/purchaseorder/PurchaseOrders"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        
        PurchaseOrderLines = []

        payload = {
            "OrderDate": record.get("transaction_date").strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            "OrderNumber": record.get("id"),
            "Supplier": record.get("supplier_remoteId"),
            "Warehouse": self.config.get("warehouse_uuid"),
            "PurchaseOrderLines": PurchaseOrderLines
        }

        if "line_items" in record:
            record["line_items"] = ast.literal_eval(record["line_items"])
            for item in record["line_items"]:
                line_item = {}
                line_item["Item"] = item.get("product_remoteId")
                line_item["QuantityInPurchaseUnits"] = item.get("quantity")
                receipt_date = record.get("created_at") if record.get("created_at") else record.get("syncedDate") 
                if receipt_date:
                    line_item["ReceiptDate"] = receipt_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

                PurchaseOrderLines.append(line_item)

            return payload
        else:
            return None

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        endpoint = "/purchaseorder/PurchaseOrders"
        state_updates = dict()
        if record:
            try:
                if record.get("buy_order_remoteId"):
                    pass
                else:
                    response = self.request_api(
                        "POST", endpoint=endpoint, request_data=record
                    )
                    res_json = xmltodict.parse(response.text)
                    id = res_json["entry"]["content"]["m:properties"]["d:PurchaseOrderID"]
                    self.logger.info(f"{self.name} created with id: {id}")
            except:
                raise KeyError
            return id, True, state_updates

class UpdateInventory(ExactSink):

    endpoint = "update_inventory"
    name = "UpdateInventory"
    endpoint = "UpdateInventory"

    def preprocess_record(self, record: dict, context: dict) -> None:
        pass
    
    def upsert_record(self, record: dict, context: dict) -> None:
        pass

