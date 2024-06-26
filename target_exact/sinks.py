"""Exact target sink class, which handles writing streams."""


import ast
import base64
import xmltodict
import json
import datetime
from pendulum import parse

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
            "Code": record.get("vendorCode"),
            "CodeAtSupplier": record.get("vendorNumber"),
            "IsSupplier": True,
            "PurchaseCurrency": record.get("currency"),
            "VATNumber": record.get("taxPayerNumber"),
            "PaymentConditionPurchase": record.get("paymentTerm"),
            "Id": record.get("id")
        }

        bankAccounts = record.get("bankAccounts")
        if bankAccounts:
            payload["bankAccounts"] = bankAccounts

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
        method = "POST"
        endpoint = self.endpoint
        action = "created"
        if record:
            bankAccounts = record.pop("bankAccounts", None)
            # check if there is id to update or create the record
            id = record.pop("Id", None)
            if id:
                endpoint = f"{self.endpoint}(guid'{id}')"
                method = "PUT"
                state_updates = {"is_updated": True}
                action = "updated"
            # send request
            response = self.request_api(
                method, endpoint=endpoint, request_data=record
            )
            # send bank account data if exists
            if bankAccounts:
                for bankAccount in bankAccounts: # how not to send dupplicated bank accounts
                    bank_account = bankAccount.get("accountNumber")
                    # check if bankAccount already exists
                    bank_accounts_endpoint = f"/crm/BankAccounts?$filter=BankAccount eq '{bank_account}' and Account eq '{id}'"
                    bank_acct = self.request_api("GET", endpoint=bank_accounts_endpoint)

                    if not bank_acct:
                        ba_payload = {
                            "BankAccount": bankAccount.get("accountNumber"),
                            "BankAccountHolderName": bankAccount.get("holderName"),
                            "BICCode": bankAccount.get("swiftCode")
                        }
                        bank_acct_response = self.request_api(
                            "POST", endpoint="/crm/BankAccounts", request_data=ba_payload
                        )
            # get new id if it's a new supplier else use id used for update
            if response.status_code == 201:
                res_json = xmltodict.parse(response.text)
                id = res_json["entry"]["content"]["m:properties"]["d:ID"]["#text"]
                self.logger.info(f"{self.name} {action} with id: {id}")
            return id, True, state_updates


class ProductsSink(ExactSink):
    """Qls target sink class."""

    name = "products"
    endpoint = "/logistics/Items"
    def search_product(self,product_code):
        id = None
        product_endpoint = f"/logistics/Items?$filter=Code eq '{product_code}'"
        product = self.request_api("GET", endpoint=product_endpoint)
        product_json = xmltodict.parse(product.text)
        products = product_json["feed"].get("entry")
        if products and len(products):
            if type(products) is dict:
                id = products["content"]["m:properties"]["d:ID"]["#text"]
            else:
                id = products[0]["content"]["m:properties"]["d:ID"]["#text"]
            
        return id
    def preprocess_record(self, record: dict, context: dict) -> dict:

        if record.get("division") and not self.current_division:
            self.endpoint = f"{record.get('division')}/{self.endpoint}"

        payload = {
            "Description": record.get("name"),
            "ExtraDescription": record.get("description"),
            "Code": record.get("sku",record.get("code")),
            "AverageCost": record.get("cost"),
            "IsSalesItem": True,# Indicate if the item is a sales item
            "IsPurchaseItem": True,# Indicate if the item is a purchase item
        }
        product_search = self.search_product(payload['Code'])
        if product_search:
            payload.update({"id":product_search})

        return payload

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        if record:
            method_type = "POST"
            endpoint = self.endpoint
            if "id" in record:
                method_type = "PUT"
                endpoint = f"{endpoint}(guid'{record['id']}')"
                id = record['id']
                del record['id']
            try:
                response = self.request_api(
                    method_type, endpoint=endpoint, request_data=record
                )
                if response.status_code==204:
                    state_updates['updated'] = True
                    state_updates['existing'] = True
                    state_updates['success'] = True
                else:    
                    res_json = xmltodict.parse(response.text)
                    id = res_json["entry"]["content"]["m:properties"]["d:ID"]["#text"]
                self.logger.info(f"{self.name} created with id: {id}")
            except:
                raise KeyError
            return id, True, state_updates

class ItemsSink(ProductsSink):
    name = "Items"
class PurchaseInvoicesSink(ExactSink):
    """Qls target sink class."""

    name = "PurchaseInvoices"
    endpoint = "/purchase/PurchaseInvoices"
    def get_journal_code(self):
        code = None
        endpoint = (
            f"/financial/Journals?$filter=Description eq 'Purchase journal'"
        )
        response = self.request_api("GET", endpoint=endpoint)
        detail = xmltodict.parse(response.text)
        journals = detail["feed"].get("entry")
        if journals is not None:
            if type(journals) is dict:
                    code = journals["content"]["m:properties"]["d:Code"]
            else:
                code = journals[0]["content"]["m:properties"]["d:Code"]
            return code
    def preprocess_record(self, record: dict, context: dict) -> dict:

        if record.get("division") and not self.current_division:
            self.endpoint = f"{record.get('division')}/{self.endpoint}"

        payload = {
            "Currency": record.get("currency"),
            "DueDate": record.get("dueDate"),
            # "createdAt": record.get("InvoiceDate"),
            "Description": record.get("description"),
            "YourRef": record.get("invoiceNumber"),
            "InvoiceDate": record.get("createdAt"),
            "Type": record.get("type"),
            "Journal": record.get("journal"),
        }
        purchase_id = record.get("purchaseNumber")
        if purchase_id: 
            purchase_id = int(purchase_id)
                
        if purchase_id and record.get("invoiceNumber"):
            payload.update({"YourRef": f"{purchase_id}-{record.get('invoiceNumber')}"})
        else:
            payload.update({"YourRef": record.get("invoiceNumber")})
        
        journal_code = self.get_journal_code()
        if journal_code:
            payload.update({"Journal":journal_code})
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
                        "Amount": line.get("totalPrice"),
                    }
                    if line.get("taxCode"):
                        invoice_line.update({"VATCode": line.get("taxCode")})

                    if line.get("taxAmount"):
                        invoice_line.update({"VATAmount": line.get("taxAmount")})    


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

            if response.status_code in [200,201]:
                state_updates["success"] = True

            res_json = xmltodict.parse(response.text)

            if "error" in res_json:
                id = None
                message = res_json["error"]["message"]["#text"]
                state_updates['error_response'] = message
                state_updates["success"] = False
                return None,False,state_updates

            if "entry" in res_json:
                id = res_json["entry"]["content"]["m:properties"]["d:ID"]["#text"]
                self.logger.info(f"{self.name} created with id: {id}")
                return id, True, state_updates

            return None, False, state_updates
        
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

    def _upload_attachment(self, attachments):
        """
        Checks if the file is a valid PDF file and uploads it to the API
        Gets all the files from the path set in config or the default path
        """
        input_path = self.config.get("input_path",'./')
        new_document_id = None

        for attachment in attachments:
            attachment_id = attachment.get("id")
            attachment_name = attachment.get("name")
            # some attachments are exported like {attachment_id}_{attachment_name} due to duplicated names
            if attachment_id:
                attachment_name = f"{attachment_id}_{attachment_name}"

            if not attachment_name:
                continue

            input_path = f"{input_path}/" if not input_path.endswith("/") else input_path
            with open(f"{input_path}{attachment_name}", "rb") as f:
                attachment = f.read()
                attachment = base64.b64encode(attachment)

            if not new_document_id:
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
        return new_document_id

    def preprocess_record(self, record: dict, context: dict) -> dict:
        if record.get("division") and not self.current_division:
            self.endpoint = f"{record.get('division')}/{self.endpoint}"

        transaction_date = record.get("transactionDate")
        period = None
        year = None
        if transaction_date:
            transac_date = parse(transaction_date)
            period = transac_date.month
            year = transac_date.year

        payload = {
            "Currency": record.get("currency"),
            "InvoiceNumber": record.get("number"),
            "YourRef": record.get("invoiceNumber"),
            "EntryDate": transaction_date,
            "Journal": record.get("journal"),
            "DueDate": record.get("dueDate"),
            "ReportingPeriod": period,
            "ReportingYear": year,
            "Description": record.get("description"),
            "PaymentReference": record.get("paymentReference"),
            "Id": record.get("id")
        }
        #get supplier id
        supplier_id = self.get_id("/crm/Accounts", {"$filter": f"Name eq '{record.get('supplierName')}'"})
        if supplier_id:
            payload["Supplier"] = supplier_id

        lookup_taxes = self.config.get("lookup_taxes") if self.config.get("lookup_taxes") == False else True
        invoice_lines = []
        lines = record.get("journalLines")
        if lines and isinstance(lines, str):
            lines = self.parse_objs(lines)
            if len(lines):
                for line in lines:
                    #get gl account id
                    account_id = self.get_id("/financial/GLAccounts", {"$filter": f"Description eq '{line.get('accountName')}'"})
                    if not account_id:
                        return {"error": f"Unable to send PurchaseEntry as GL account {line.get('accountName')} doesn't exist for record with invoiceNumber {record.get('invoiceNumber')}"}
                    # flag added because new tenants are sending exact taxCode but older tenants are sending tax name as taxCode
                    if lookup_taxes:
                        vat_code = self.get_id("/vat/VATCodes", {"$filter": f"Description eq '{line.get('taxCode')}'"}, key="Code")
                    else:
                        vat_code = line.get('taxCode')
                    invoice_line = {
                        "AmountFC": line.get("amount"),
                        "AmountDC": line.get("amount"),
                        "GLAccount": account_id,
                        "Description": line.get("description", line.get("productName")),
                        "VATCode": vat_code,
                        "CostCenter": line.get("costCenter"),
                        "CostUnit": line.get("costUnit"),
                    }

                    # optional fields
                    if line.get('projectName'):
                        project_id = self.get_id("/project/Projects", {"$filter": f"Description eq '{line.get('projectName')}'"})
                        if project_id:
                            invoice_line["Project"] = project_id
                    invoice_lines.append(invoice_line)

            payload["PurchaseEntryLines"] = invoice_lines
        payload = self.clean_payload(payload)
        if record.get("attachments"):
            record["attachments"] = json.loads(record["attachments"])
            if isinstance(record["attachments"], list):
                payload["Document"] = self._upload_attachment(record["attachments"])
            else:
                return {"error": "Attachments should be a list", "externalId": record.get("externalId")}
        
        return payload

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        endpoint = self.endpoint
        action = "created"
        method = "POST"
        if record:
            if record.get("error"):
                raise Exception(record.get("error"))
            # check if there is id to update or create the record
            id = record.pop("Id", None)
            if id:
                endpoint = f"{self.endpoint}(guid'{id}')"
                method = "PUT"
                action = "updated"
                record.pop("PurchaseEntryLines", None)
                state_updates["is_updated"] = True
            response = self.request_api(
                method, endpoint=endpoint, request_data=record
            )
            if response.status_code == 201:
                res_json = xmltodict.parse(response.text)
                id = res_json["entry"]["content"]["m:properties"]["d:EntryID"]["#text"]
            self.logger.info(f"{self.name} {action} with id: {id}")
            return id, True, state_updates

