import json
from unittest.mock import MagicMock, patch

import pytest

from target_exact.sinks import (
    EXACT_PURCHASE_CREDIT_NOTE_TYPE,
    EXACT_PURCHASE_ENTRY_TYPE,
    PurchaseCreditsSink,
    PurchaseEntriesSink,
)

BOTH_SINKS_WITH_THEIR_TYPE = [
    (PurchaseCreditsSink, EXACT_PURCHASE_CREDIT_NOTE_TYPE),
    (PurchaseEntriesSink, EXACT_PURCHASE_ENTRY_TYPE),
]


class FakeResponse:
    def __init__(self, status_code=200, text=""):
        self.status_code = status_code
        self.text = text


def make_sink_with_supplier_and_account_found(sink_class, explicit_id_is_this_type=False, config=None):
    sink = sink_class.__new__(sink_class)
    sink.logger = MagicMock()
    sink.endpoint = "/purchaseentry/PurchaseEntries"
    sink._config = config or {}
    sink.request_api = MagicMock(return_value=FakeResponse(200))

    def fake_get_id(endpoint, filter, key="ID"):
        if endpoint == "/crm/Accounts":
            return "supplier-1"
        if endpoint == "/financial/GLAccounts":
            return "gl-1"
        if endpoint == sink.endpoint:
            return "entry-1" if explicit_id_is_this_type else None
        return None

    sink.get_id = MagicMock(side_effect=fake_get_id)
    return sink


def make_record(line_amounts, **overrides):
    record = {
        "invoiceNumber": "CN-1",
        "supplierId": "supplier-1",
        "transactionDate": "2026-10-01",
        "currency": "EUR",
        "journalLines": json.dumps(
            [{"amount": a, "accountNumber": "4000", "taxCode": "1"} for a in line_amounts]
        ),
    }
    record.update(overrides)
    return record


def your_ref_lookup_finds_no_entry():
    empty_feed = {"feed": {"title": "PurchaseEntries"}}
    return patch("target_exact.sinks.xmltodict.parse", return_value=empty_feed)


def your_ref_lookup_finds_entry(entry_id):
    matching_feed = {"feed": {"entry": {"content": {"m:properties": {"d:EntryID": {"#text": entry_id}}}}}}
    return patch("target_exact.sinks.xmltodict.parse", return_value=matching_feed)


class TestLineAmounts:
    @pytest.mark.parametrize(
        "source_amount, expected_exact_amount",
        [(100, -100.0), (-50, -50.0), ("25.5", -25.5), ("-25.5", -25.5), (0, 0.0)],
    )
    def test_credit_note_line_amounts_are_always_negative(self, source_amount, expected_exact_amount):
        sink = make_sink_with_supplier_and_account_found(PurchaseCreditsSink)
        with your_ref_lookup_finds_no_entry():
            payload = sink.preprocess_record(make_record([source_amount]), {})

        line = payload["PurchaseEntryLines"][0]
        assert line["AmountFC"] == expected_exact_amount
        assert line["AmountDC"] == expected_exact_amount

    def test_credit_note_missing_amount_is_passed_through(self):
        sink = make_sink_with_supplier_and_account_found(PurchaseCreditsSink)

        assert sink._to_exact_line_amount(None) is None
        assert sink._to_exact_line_amount("") == ""

    def test_credit_note_non_numeric_amount_is_a_record_error(self):
        sink = make_sink_with_supplier_and_account_found(PurchaseCreditsSink)
        with your_ref_lookup_finds_no_entry():
            payload = sink.preprocess_record(make_record(["abc"]), {})

        assert "error" in payload

    def test_purchase_entry_line_amounts_keep_source_sign(self):
        sink = make_sink_with_supplier_and_account_found(PurchaseEntriesSink)
        with your_ref_lookup_finds_no_entry():
            payload = sink.preprocess_record(make_record([100, -50]), {})

        assert [line["AmountFC"] for line in payload["PurchaseEntryLines"]] == [100, -50]
        assert [line["AmountDC"] for line in payload["PurchaseEntryLines"]] == [100, -50]


class TestTypeOnCreate:
    @pytest.mark.parametrize("sink_class, entry_type", BOTH_SINKS_WITH_THEIR_TYPE)
    def test_new_entry_is_sent_with_its_type(self, sink_class, entry_type):
        sink = make_sink_with_supplier_and_account_found(sink_class)
        with your_ref_lookup_finds_no_entry():
            payload = sink.preprocess_record(make_record([100]), {})

        assert payload["Type"] == entry_type
        assert "Id" not in payload

    def test_update_of_matched_credit_note_does_not_resend_type(self):
        sink = make_sink_with_supplier_and_account_found(PurchaseCreditsSink)
        with your_ref_lookup_finds_entry("credit-1"):
            payload = sink.preprocess_record(make_record([100]), {})

        assert payload["Id"] == "credit-1"
        assert "Type" not in payload


class TestLookupsOnlyMatchOwnType:
    @pytest.mark.parametrize("sink_class, entry_type", BOTH_SINKS_WITH_THEIR_TYPE)
    def test_your_ref_and_supplier_lookup_filters_on_own_type(self, sink_class, entry_type):
        sink = make_sink_with_supplier_and_account_found(sink_class)
        with your_ref_lookup_finds_no_entry():
            sink._find_existing_purchase_entry_id("CN-1", "supplier-1")

        params = sink.request_api.call_args.kwargs["params"]
        assert params["$filter"] == (
            f"YourRef eq 'CN-1' and Supplier eq guid'supplier-1' and Type eq {entry_type}"
        )

    @pytest.mark.parametrize("sink_class, entry_type", BOTH_SINKS_WITH_THEIR_TYPE)
    def test_explicit_id_check_filters_on_own_type(self, sink_class, entry_type):
        sink = make_sink_with_supplier_and_account_found(sink_class, explicit_id_is_this_type=True)

        assert sink._is_existing_entry_of_this_type("entry-1") is True
        endpoint, filter = sink.get_id.call_args.args[:2]
        assert endpoint == sink.endpoint
        assert filter["$filter"] == f"EntryID eq guid'entry-1' and Type eq {entry_type}"

    def test_invoice_id_sent_as_credit_note_is_rejected_before_any_request(self):
        sink = make_sink_with_supplier_and_account_found(PurchaseCreditsSink, explicit_id_is_this_type=False)
        payload = sink.preprocess_record(make_record([100], id="invoice-1"), {})

        assert "invoice-1" in payload["error"]
        assert f"Type {EXACT_PURCHASE_CREDIT_NOTE_TYPE}" in payload["error"]
        sink.request_api.assert_not_called()

    def test_credit_note_id_sent_as_credit_note_is_updated(self):
        sink = make_sink_with_supplier_and_account_found(PurchaseCreditsSink, explicit_id_is_this_type=True)
        payload = sink.preprocess_record(make_record([100], id="credit-1"), {})

        assert payload["Id"] == "credit-1"
        assert "Type" not in payload
        assert payload["PurchaseEntryLines"][0]["AmountFC"] == -100.0
