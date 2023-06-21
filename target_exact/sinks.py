"""Exact target sink class, which handles writing streams."""


from target_exact.client import ExactSink
import ast
import xmltodict
import json
from target_exact.constants import countries


class BuyOrdersSink(ExactSink):
    """Qls target sink class."""

    name = "BuyOrders"
    endpoint = "/purchaseorder/PurchaseOrders"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        PurchaseOrderLines = []

        payload = {
            "OrderDate": record.get("transaction_date").strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            "OrderNumber": record.get("id"),
            "Supplier": record.get("supplier_remoteId"),
            "PurchaseOrderLines": PurchaseOrderLines,
            "buy_order_remoteId": record.get("remoteId")
        }

        if "line_items" in record:
            record["line_items"] = ast.literal_eval(record["line_items"])
            for item in record["line_items"]:
                line_item = {}
                line_item["Item"] = item.get("product_remoteId")
                line_item["QuantityInPurchaseUnits"] = item.get("quantity")
                receipt_date = (
                    record.get("created_at")
                    if record.get("created_at")
                    else record.get("syncedDate")
                )
                if receipt_date:
                    line_item["ReceiptDate"] = receipt_date.strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    )

                PurchaseOrderLines.append(line_item)

            return payload
        else:
            return None

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        endpoint = "/purchaseorder/PurchaseOrders"
        state_updates = dict()
        if record:
            if record.get("buy_order_remoteId"):
                # TODO: Why is this block even here??
                id = record.get("buy_order_remoteId")
            else:
                del record['buy_order_remoteId']
                warehouse_uuid = self.config.get("warehouse_uuid")
                if warehouse_uuid:
                    record["Warehouse"] = warehouse_uuid
                else:
                    try:
                        warehouse_uuid = self.default_warehouse_uuid
                        record["Warehouse"] = warehouse_uuid
                    except Exception as e:
                        self.update_state(
                            {"error": "Warehouse uuid missing in config file"}
                        )
                        raise e

                response = self.request_api(
                    "POST", endpoint=endpoint, request_data=record
                )
                self.logger.info(f"response from api: {response.text}")
                res_json = xmltodict.parse(response.text)
                id = res_json["entry"]["content"]["m:properties"]["d:PurchaseOrderID"][
                    "#text"
                ]
                self.logger.info(f"{self.name} created with id: {id}")

            self.logger.info(f"Returning {id}, True, {state_updates}")
            return id, True, state_updates


class UpdateInventory(ExactSink):
    endpoint = "update_inventory"
    name = "UpdateInventory"
    endpoint = "UpdateInventory"

    def preprocess_record(self, record: dict, context: dict) -> None:
        pass

    def upsert_record(self, record: dict, context: dict) -> None:
        state_updates = dict()
        id = ""
        return id, True, state_updates



class SuppliersSink(ExactSink):
    """Qls target sink class."""

    name = "Suppliers"
    endpoint = "/crm/Accounts"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        payload = {
            "Name": record.get("vendorName"),
            "CodeAtSupplier": record.get("vendorNumber"),
        }

        phones = record.get("phoneNumbers")
        if phones and isinstance(phones, str):
            phones = ast.literal_eval(phones)
            if len(phones):
                payload["Phone"] = phones[0]["number"]

        record_address = record.get("addresses")
        if record_address and isinstance(record_address, str):
            record_address = record_address.replace("null", '""')
            record_address = ast.literal_eval(record_address)
            if len(record_address):
                record_address = record_address[0]
                payload["AddressLine1"] = record_address.get("line1")
                payload["City"] = record_address.get("city")
                payload["State"] = record_address.get("state")

                country = record_address.get("country")
                if country:
                    if len(country) == 2:
                        payload["Country"] = country
                    elif country in countries.keys():
                        payload["Country"] = countries[country]

        return payload

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        endpoint = "/crm/Accounts"
        state_updates = dict()
        if record:
            try:
                response = self.request_api(
                    "POST", endpoint=endpoint, request_data=record
                )
                res_json = xmltodict.parse(response.text)
                id = res_json["entry"]["content"]["m:properties"]["d:ID"]["#text"]
                self.logger.info(f"{self.name} created with id: {id}")
            except:
                raise KeyError
            return id, True, state_updates


class ItemsSink(ExactSink):
    """Qls target sink class."""

    name = "products"
    endpoint = "/logistics/Items"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        payload = {
            "Description": record.get("name"),
            "ExtraDescription": record.get("description"),
            "Code": record.get("sku"),
            "AverageCost": record.get("cost"),
        }

        return payload

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        endpoint = "/logistics/Items"
        state_updates = dict()
        if record:
            try:
                response = self.request_api(
                    "POST", endpoint=endpoint, request_data=record
                )
                res_json = xmltodict.parse(response.text)
                id = res_json["entry"]["content"]["m:properties"]["d:ID"]["#text"]
                self.logger.info(f"{self.name} created with id: {id}")
            except:
                raise KeyError
            return id, True, state_updates


class PurchaseInvoicesSink(ExactSink):
    """Qls target sink class."""

    name = "PurchaseInvoices"
    endpoint = "/purchase/PurchaseInvoices"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        payload = {
            "Currency": record.get("currency"),
            "DueDate": record.get("dueDate"),
            "YourRef": record.get("invoiceNumber"),
            "createdAt": record.get("InvoiceDate"),
            "Type": 8033,
            "Journal": "95",
        }

        supplier_endpoint = (
            f"/crm/Accounts?$filter=Name eq '{record.get('supplierName')}'"
        )
        supplier = self.request_api("GET", endpoint=supplier_endpoint)
        supplier_json = xmltodict.parse(supplier.text)
        suppliers = supplier_json["feed"].get("entry")
        if suppliers and len(suppliers):
            if type(suppliers) is dict:
                id = suppliers["content"]["m:properties"]["d:ID"]["#text"]
            else:
                id = suppliers[0]["content"]["m:properties"]["d:ID"]["#text"]
            payload["Supplier"] = id
        else:
            return None

        invoice_lines = []
        lines = record.get("lineItems")
        if lines and isinstance(lines, str):
            lines = lines.replace("null", '""')
            lines = lines.replace("false", "False")
            lines = lines.replace("true", "True")
            lines = ast.literal_eval(lines)
            if len(lines):
                for line in lines:
                    invoice_line = {
                        "UnitPrice": line.get("unitPrice"),
                        "Quantity": line.get("quantity"),
                        "VATCode": line.get("taxCode"),
                        "VATAmount": line.get("taxAmount"),
                        "Amount": line.get("totalPrice"),
                    }

                    product_endpoint = f"/logistics/Items?$filter=Description eq '{line.get('productName')}'"
                    product = self.request_api("GET", endpoint=product_endpoint)
                    product_json = xmltodict.parse(product.text)
                    products = product_json["feed"].get("entry")
                    if products and len(products):
                        if type(products) is dict:
                            id = products["content"]["m:properties"]["d:ID"]["#text"]
                        else:
                            id = products[0]["content"]["m:properties"]["d:ID"]["#text"]
                        invoice_line["Item"] = id
                        invoice_lines.append(invoice_line)
                    else:
                        pass

            payload["PurchaseInvoiceLines"] = invoice_lines

        return payload

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        endpoint = "/purchase/PurchaseInvoices"
        state_updates = dict()
        if record:
            try:
                response = self.request_api(
                    "POST", endpoint=endpoint, request_data=record
                )
                res_json = xmltodict.parse(response.text)
                id = res_json["entry"]["content"]["m:properties"]["d:ID"]["#text"]
                self.logger.info(f"{self.name} created with id: {id}")
            except:
                raise KeyError
            return id, True, state_updates
