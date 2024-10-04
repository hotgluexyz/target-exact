"""Exact target sink class, which handles writing streams."""


import ast
import base64
import xmltodict
import json
import datetime
from pendulum import parse

from target_exact.client import ExactSink
from target_exact.constants import SALES_ORDER_STATUS, countries
from target_exact.exceptions import (
    InvalidOrderNumberError,
    MissingItemError,
    InvalidOrderedByError,
)


class BuyOrdersSink(ExactSink):
    """Qls target sink class."""

    name = "BuyOrders"
    endpoint = "/purchaseorder/PurchaseOrders"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        PurchaseOrderLines = []

        receipt_date = (
            record.get("created_at")
            if record.get("created_at")
            else datetime.datetime.now(datetime.timezone.utc)
        )

        payload = {
            "OrderDate": record.get("transaction_date").strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            "Supplier": record.get("supplier_remoteId"),
            "PurchaseOrderLines": PurchaseOrderLines,
            "buy_order_remoteId": record.get("remoteId"),
        }

        if "id" in record:
            payload["OrderNumber"] = record.get("id")

        if "reference" in record:
            payload["YourRef"] = record.get("reference")

        if receipt_date:
            if isinstance(receipt_date, str):
                receipt_date = parse(receipt_date)

            receipt_date = receipt_date.strftime(
                            "%Y-%m-%dT%H:%M:%S.%fZ"
                        )
            payload["ReceiptDate"] = receipt_date

        if "line_items" in record:
            record["line_items"] = json.loads(record["line_items"])
            for item in record["line_items"]:
                line_item = {}
                line_item["Item"] = item.get("product_remoteId")
                if not item.get("lot_size") or item.get("lot_size") == False:
                    item["lot_size"] = 1
                line_item["QuantityInPurchaseUnits"] = item.get("quantity") / item.get("lot_size", 1)
                if item.get("receipt_date"):
                    line_item["ReceiptDate"] = item.get("receipt_date")
                elif receipt_date:
                    line_item["ReceiptDate"] = receipt_date

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
        return {}

    def upsert_record(self, record: dict, context: dict) -> None:
        state_updates = dict()
        id = "id"
        return id, True, state_updates



class SuppliersSink(ExactSink):
    """Qls target sink class."""

    name = "Suppliers"
    endpoint = "/crm/Accounts"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        if record.get("division") and not self.current_division:
            self.endpoint = f"{record.get('division')}/{self.endpoint}"

        payload = {
            "Name": record.get("vendorName"),
            "CodeAtSupplier": record.get("vendorNumber"),
        }

        phones = record.get("phoneNumbers")
        if phones and isinstance(phones, str):
            phones = ast.literal_eval(phones) #TODO: Change this to json.loads
            if len(phones):
                payload["Phone"] = phones[0]["number"]

        record_address = record.get("addresses")
        if record_address and isinstance(record_address, str):
            record_address = record_address.replace("null", '""')
            record_address = ast.literal_eval(record_address) # TODO: Change this to json.loads
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
        state_updates = dict()
        if record:
            response = self.request_api(
                "POST", endpoint=self.endpoint, request_data=record
            )
            res_json = xmltodict.parse(response.text)
            id = res_json["entry"]["content"]["m:properties"]["d:ID"]["#text"]
            self.logger.info(f"{self.name} created with id: {id}")
            return id, True, state_updates


class ItemsSink(ExactSink):
    """Qls target sink class."""

    name = "products"
    endpoint = "/logistics/Items"

    def preprocess_record(self, record: dict, context: dict) -> dict:

        if record.get("division") and not self.current_division:
            self.endpoint = f"{record.get('division')}/{self.endpoint}"

        payload = {
            "Description": record.get("name"),
            "ExtraDescription": record.get("description"),
            "Code": record.get("sku"),
            "AverageCost": record.get("cost"),
        }

        return payload

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        if record:
            response = self.request_api(
                "POST", endpoint=self.endpoint, request_data=record
            )
            res_json = xmltodict.parse(response.text)
            id = res_json["entry"]["content"]["m:properties"]["d:ID"]["#text"]
            self.logger.info(f"{self.name} created with id: {id}")
            return id, True, state_updates


class PurchaseInvoicesSink(ExactSink):
    """Qls target sink class."""

    name = "PurchaseInvoices"
    endpoint = "/purchase/PurchaseInvoices"

    def preprocess_record(self, record: dict, context: dict) -> dict:

        if record.get("division") and not self.current_division:
            self.endpoint = f"{record.get('division')}/{self.endpoint}"

        payload = {
            "Currency": record.get("currency"),
            "DueDate": record.get("dueDate"),
            "YourRef": record.get("invoiceNumber"),
            "InvoiceDate": record.get("createdAt"),
            "Type": record.get("type"),
            "Journal": record.get("journal"),
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
            lines = ast.literal_eval(lines) # TODO: Change to json.loads
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
        state_updates = dict()
        if record:
            response = self.request_api(
                "POST", endpoint=self.endpoint, request_data=record
            )
            res_json = xmltodict.parse(response.text)
            id = res_json["entry"]["content"]["m:properties"]["d:ID"]["#text"]
            self.logger.info(f"{self.name} created with id: {id}")
            return id, True, state_updates


class PurchaseEntriesSink(ExactSink):

    name = "PurchaseEntries"
    endpoint = "/purchaseentry/PurchaseEntries"

    def _create_document(self):
        # Creates a document for the journal entry
        document_payload = {
            "Subject": "Journal Entry",
            "Type": "20",
        }

        document = self.request_api("POST", endpoint="/documents/Documents", request_data=document_payload)
        document_json = xmltodict.parse(document.text)
        document_id = document_json["entry"]["content"]["m:properties"]["d:ID"]["#text"]
        return document_id

    def _upload_attachment(self, attachment_name, attachment_id=None):
        """
        Checks if the file is a valid PDF file and uploads it to the API
        Gets all the files from the path set in config or the default path
        """
        input_path = self.config.get("input_path",'./')

        # some attachments are exported like {attachment_id}_{attachment_name} due to duplicated names
        if attachment_id:
            attachment_name = f"{attachment_id}_{attachment_name}"

        if not attachment_name:
            return None

        if not attachment_name.endswith(".pdf"):
            self.logger.info(f"Attachment {attachment_name} is not a PDF file")
            return None

        with open(f"{input_path}{attachment_name}", "rb") as f:
            attachment = f.read()
            attachment = base64.b64encode(attachment)

        new_document_id = self._create_document()

        attachment_payload = {
            "Attachment": attachment,
            "FileName": attachment_name,
            "Document": new_document_id,
        }

        attachment = self.request_api(
            "POST", endpoint="/documents/DocumentAttachments",
            request_data=attachment_payload
        )

        attachment_json = xmltodict.parse(attachment.text)
        attachment_id = attachment_json["entry"]["content"]["m:properties"]["d:ID"]["#text"]
        return attachment_id

    def preprocess_record(self, record: dict, context: dict) -> dict:

        if record.get("division") and not self.current_division:
            self.endpoint = f"{record.get('division')}/{self.endpoint}"

        payload = {
            "Currency": record.get("currency"),
            "YourRef": record.get("id"),
            "EntryDate": record.get("transactionDate"),
            "Journal": record.get("journal"),
        }
        #get supplier id
        supplier_id = self.get_id("/crm/Accounts", {"$filter": f"Name eq '{record.get('supplierName')}'"})
        if supplier_id:
            payload["Supplier"] = supplier_id

        invoice_lines = []
        lines = record.get("journalLines")
        if lines and isinstance(lines, str):
            lines = self.parse_objs(lines)
            if len(lines):
                for line in lines:
                    #get gl account id
                    account_id = self.get_id("/financial/GLAccounts", {"$filter": f"Description eq '{line.get('accountName')}'"})
                    if not account_id:
                        self.logger.info("skipping journal entry line due to missing or inexistent account name")
                        continue
                    invoice_line = {
                        "AmountFC": line.get("amount"),
                        "AmountDC": line.get("amount"),
                        "GLAccount": account_id
                    }
                    invoice_lines.append(invoice_line)

            payload["PurchaseEntryLines"] = invoice_lines
        payload = self.clean_payload(payload)
        if record.get("attachments"):
            record["attachments"] = json.loads(record["attachments"])
            if len(record["attachments"]) > 0:
                payload["Document"] = self._upload_attachment(record["attachments"][0]["name"], record["attachments"][0].get("id"))
        return payload

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        if record:
            response = self.request_api(
                "POST", endpoint=self.endpoint, request_data=record
            )
            res_json = xmltodict.parse(response.text)
            id = res_json["entry"]["content"]["m:properties"]["d:EntryID"]["#text"]
            self.logger.info(f"{self.name} created with id: {id}")
            return id, True, state_updates


class SalesOrdersSink(ExactSink):

    name = "SalesOrders"
    endpoint = "/salesorder/SalesOrders"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            if record.get("division") and not self.current_division:
                self.endpoint = f"{record.get('division')}/{self.endpoint}"
            
            order_number = record.get('order_number')
            order_id = record.get("id")

            try:
                int(order_number)
            except (ValueError, TypeError):
                raise InvalidOrderNumberError(f"OrderNumber should be int. OrderID {order_id} and OrderNumber {order_number}")
            
            accounts_endpoint = '/crm/Accounts'
            if not (ordered_by := self.get_id(accounts_endpoint, {"$filter": f"Name eq '{record.get('customer_name')}'"})):
                raise InvalidOrderedByError(f"Customer Name {record.get('customer_name')} not found. " + \
                                            f"OrderID {order_id}, OrderNumber {order_number}")

            order_lines = []
            for item in record.get("line_items", [{}]):
                
                item_id = None
                item_endpoint = "/logistics/Items"
                if product_id := item.get("product_id"):
                    item_id = self.get_id(item_endpoint, {"$filter": f"ID eq guid'{product_id}'"})
                if not item_id:
                    item_id = self.get_id(item_endpoint, {"$filter": f"Code eq '{item.get('sku')}'"})
                if not item_id:
                    item_id = self.get_id(item_endpoint, {"$filter": f"Code eq '{item.get('product_name')}'"})
                if not item_id:
                    item_id = self.get_id(item_endpoint, {"$filter": f"Description eq '{item.get('product_name')}'"})
                if not item_id:
                    raise MissingItemError(f"Item not found for SKU {item.get('sku')} and Name {item.get('product_name')}. " + \
                                    f"OrderID {order_id} and OrderNumber {record.get('order_number')}.")
                    
                order_line = {
                    "OrderID": order_id,
                    "Item": item_id,
                    "Quantity": item.get("quantity"),
                    "NetPrice": item.get("unit_price"),
                    "VATCode": item.get("tax_code"),
                    "Description": item.get("product_name"),
                    "Notes": item.get("sku"),
                }
                if item.get("discount_amount"):
                    order_line["Discount"] = item.get("discount_amount")/item.get("unit_price")
                order_lines.append(order_line)
            

            payload = {
                "YourRef": order_id,
                "SalesOrderLines": order_lines,
                "ApprovalStatus": SALES_ORDER_STATUS.get(record.get("status"), 0),
                "OrderDate": record.get("transaction_date", None) or record.get("created_at"),
                "OrderNumber": record.get("order_number"),
                "AmountDiscount": record.get("total_discount"),
                "Description": record.get("order_notes"),
                "DeliverTo": self.get_id(accounts_endpoint, {"$filter": f"Name eq '{record.get('shipping_name')}'"}),
                "InvoiceTo": self.get_id(accounts_endpoint, {"$filter": f"Name eq '{record.get('billing_name')}'"}),
                "OrderedBy": ordered_by,
            }
            return payload
        except Exception as exc:
            return {"error": repr(exc)}
    
    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        if record:
            if record.get("error"):
                raise Exception(record.get("error"))
            response = self.request_api(
                "POST", endpoint=self.endpoint, request_data=record
            )
            res_json = xmltodict.parse(response.text)
            id = res_json["entry"]["content"]["m:properties"]["d:OrderID"]["#text"]
            self.logger.info(f"{self.name} created with id: {id}")
            return id, True, state_updates