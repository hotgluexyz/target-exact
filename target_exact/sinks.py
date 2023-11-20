"""Exact target sink class, which handles writing streams."""


import ast
import base64
import xmltodict
import json
import datetime

from target_exact.client import ExactSink
from target_exact.constants import countries



class BuyOrdersSink(ExactSink):
    """Qls target sink class."""

    name = "BuyOrders"
    endpoint = "/purchaseorder/PurchaseOrders"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        PurchaseOrderLines = []

        receipt_date = (
            record.get("created_at")
            if record.get("created_at")
            else datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
        )

        payload = {
            "OrderDate": record.get("transaction_date").strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            "OrderNumber": record.get("id"),
            "Supplier": record.get("supplier_remoteId"),
            "PurchaseOrderLines": PurchaseOrderLines,
            "buy_order_remoteId": record.get("remoteId"),
        }

        if receipt_date:
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
                if receipt_date:
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

    def _upload_attachment(self, attachment_name):
        """
        Checks if the file is a valid PDF file and uploads it to the API
        Gets all the files from the path set in config or the default path
        """
        input_path = self.config.get("input_path",'./')

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
                payload["Document"] = self._upload_attachment(record["attachments"][0]["name"])
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
