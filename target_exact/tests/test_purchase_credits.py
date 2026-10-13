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


def make_sink_with_supplier_and_account_found(sink_class):
    sink = sink_class.__new__(sink_class)
    sink.logger = MagicMock()
    sink.endpoint = "/purchaseentry/PurchaseEntries"
    sink._config = {}
    sink.request_api = MagicMock()
    supplier_and_account_ids = {"/crm/Accounts": "supplier-1", "/financial/GLAccounts": "gl-1"}
    sink.get_id = MagicMock(side_effect=lambda endpoint, filter, key="ID": supplier_and_account_ids.get(endpoint))
    return sink


def make_record(line_amounts):
    return {
        "invoiceNumber": "CN-1",
        "supplierId": "supplier-1",
        "transactionDate": "2026-10-01",
        "currency": "EUR",
        "journalLines": json.dumps(
            [{"amount": a, "accountNumber": "4000", "taxCode": "1"} for a in line_amounts]
        ),
    }


def your_ref_lookup_finds_no_entry():
    empty_feed = {"feed": {"title": "PurchaseEntries"}}
    return patch("target_exact.sinks.xmltodict.parse", return_value=empty_feed)


class TestLineAmounts:
    @pytest.mark.parametrize(
        "source_amount, expected_exact_amount",
        [(100, -100.0), (-50, -50.0), ("25.5", -25.5), ("-25.5", -25.5)],
    )
    def test_credit_note_line_amounts_are_always_negative(self, source_amount, expected_exact_amount):
        sink = make_sink_with_supplier_and_account_found(PurchaseCreditsSink)
        with your_ref_lookup_finds_no_entry():
            payload = sink.preprocess_record(make_record([source_amount]), {})

        line = payload["PurchaseEntryLines"][0]
        assert line["AmountFC"] == expected_exact_amount
        assert line["AmountDC"] == expected_exact_amount

    def test_purchase_entry_line_amounts_keep_source_sign(self):
        sink = make_sink_with_supplier_and_account_found(PurchaseEntriesSink)
        with your_ref_lookup_finds_no_entry():
            payload = sink.preprocess_record(make_record([100, -50]), {})

        assert [line["AmountFC"] for line in payload["PurchaseEntryLines"]] == [100, -50]
        assert [line["AmountDC"] for line in payload["PurchaseEntryLines"]] == [100, -50]


class TestEachSinkUsesItsOwnType:
    @pytest.mark.parametrize("sink_class, entry_type", BOTH_SINKS_WITH_THEIR_TYPE)
    def test_entry_is_sent_with_its_type(self, sink_class, entry_type):
        sink = make_sink_with_supplier_and_account_found(sink_class)
        with your_ref_lookup_finds_no_entry():
            payload = sink.preprocess_record(make_record([100]), {})

        assert payload["Type"] == entry_type

    @pytest.mark.parametrize("sink_class, entry_type", BOTH_SINKS_WITH_THEIR_TYPE)
    def test_your_ref_and_supplier_lookup_only_matches_own_type(self, sink_class, entry_type):
        sink = make_sink_with_supplier_and_account_found(sink_class)
        with your_ref_lookup_finds_no_entry():
            sink._find_existing_purchase_entry_id("CN-1", "supplier-1")

        params = sink.request_api.call_args.kwargs["params"]
        assert params["$filter"] == (
            f"YourRef eq 'CN-1' and Supplier eq guid'supplier-1' and Type eq {entry_type}"
        )
