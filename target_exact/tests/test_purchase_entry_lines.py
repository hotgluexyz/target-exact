"""Regression tests for PurchaseEntriesSink line replacement on update.

Exact's OData API doesn't support replacing the nested PurchaseEntryLines
collection via a header PUT, so updates delete the old lines and recreate
them individually (see _replace_purchase_entry_lines). These tests exercise
that flow directly against the real sink code, mocking only request_api and
xmltodict.parse - not a reimplementation of the logic.
"""

from unittest.mock import MagicMock, patch

import pytest

from target_exact.sinks import PurchaseEntriesSink


class FakeResponse:
    def __init__(self, status_code=200, text=""):
        self.status_code = status_code
        self.text = text


def make_sink(request_side_effect):
    """Build a PurchaseEntriesSink without running HotglueSink.__init__."""
    sink = PurchaseEntriesSink.__new__(PurchaseEntriesSink)
    sink.request_api = MagicMock(side_effect=request_side_effect)
    sink.logger = MagicMock()
    sink.name = "PurchaseEntries"
    sink.endpoint = "/purchaseentry/PurchaseEntries"
    return sink


def entry_ids_feed(ids):
    """Mimic xmltodict output for a GET on PurchaseEntryLines."""
    if not ids:
        return {"feed": None}
    entries = [{"content": {"m:properties": {"d:ID": {"#text": i}}}} for i in ids]
    return {"feed": {"entry": entries[0] if len(entries) == 1 else entries}}


def created_entry(id_):
    """Mimic xmltodict output for a POST creating a single line."""
    return {"entry": {"content": {"m:properties": {"d:ID": {"#text": id_}}}}}


class TestGetExistingLineIds:
    def test_empty_feed_returns_empty_list(self):
        sink = make_sink([FakeResponse(200, "<feed/>")])
        with patch("target_exact.sinks.xmltodict.parse", return_value={"feed": None}):
            assert sink._get_existing_line_ids("entry-1") == []

    def test_single_entry_not_wrapped_in_list(self):
        sink = make_sink([FakeResponse(200, "<feed/>")])
        with patch(
            "target_exact.sinks.xmltodict.parse",
            return_value=entry_ids_feed(["line-a"]),
        ):
            assert sink._get_existing_line_ids("entry-1") == ["line-a"]

    def test_multiple_entries(self):
        sink = make_sink([FakeResponse(200, "<feed/>")])
        with patch(
            "target_exact.sinks.xmltodict.parse",
            return_value=entry_ids_feed(["line-a", "line-b"]),
        ):
            assert sink._get_existing_line_ids("entry-1") == ["line-a", "line-b"]


class TestReplacePurchaseEntryLines:
    def test_replaces_lines_create_before_delete(self):
        sink = make_sink(
            [
                FakeResponse(200),  # GET existing lines
                FakeResponse(201),  # POST new line 1
                FakeResponse(201),  # POST new line 2
                FakeResponse(204),  # DELETE old line 1
                FakeResponse(204),  # DELETE old line 2
            ]
        )
        with patch(
            "target_exact.sinks.xmltodict.parse",
            side_effect=[
                entry_ids_feed(["old-1", "old-2"]),
                created_entry("new-1"),
                created_entry("new-2"),
            ],
        ):
            created = sink._replace_purchase_entry_lines(
                "entry-1", [{"AmountFC": 100}, {"AmountFC": 200}]
            )

        assert created == ["new-1", "new-2"]
        methods = [c.args[0] for c in sink.request_api.call_args_list]
        assert methods == ["GET", "POST", "POST", "DELETE", "DELETE"]

    def test_zero_existing_lines_skips_delete(self):
        sink = make_sink(
            [
                FakeResponse(200),  # GET existing lines -> none
                FakeResponse(201),  # POST new line
            ]
        )
        with patch(
            "target_exact.sinks.xmltodict.parse",
            side_effect=[entry_ids_feed([]), created_entry("new-1")],
        ):
            created = sink._replace_purchase_entry_lines("entry-1", [{"AmountFC": 100}])

        assert created == ["new-1"]
        assert sink.request_api.call_count == 2
        assert [c.args[0] for c in sink.request_api.call_args_list] == ["GET", "POST"]

    def test_partial_delete_failure_raises_with_progress_counts(self):
        def side_effect(method, endpoint=None, request_data=None, params=None):
            if method == "GET":
                return FakeResponse(200)
            if method == "POST":
                return FakeResponse(201)
            if method == "DELETE":
                if endpoint == "/purchaseentry/PurchaseEntryLines(guid'old-2')":
                    raise Exception("500 server error")
                return FakeResponse(204)

        sink = PurchaseEntriesSink.__new__(PurchaseEntriesSink)
        sink.logger = MagicMock()
        sink.request_api = MagicMock(side_effect=side_effect)

        with patch(
            "target_exact.sinks.xmltodict.parse",
            side_effect=[
                entry_ids_feed(["old-1", "old-2", "old-3"]),
                created_entry("new-1"),
            ],
        ):
            with pytest.raises(Exception, match=r"deleted 1/3 before failure"):
                sink._replace_purchase_entry_lines("entry-1", [{"AmountFC": 100}])

        delete_calls = [
            c for c in sink.request_api.call_args_list if c.args[0] == "DELETE"
        ]
        assert len(delete_calls) == 2  # stopped after the failing second delete


class TestUpsertRecordEmptyNewLines:
    """Guards the bug found reviewing olehvoloshin/target-exact#1: replacing
    lines with an empty new_lines list has no line to fall back on, so with
    2+ existing lines the DELETE loop hits Exact's "at least one line"
    constraint partway through and leaves the entry with some old lines
    permanently gone and nothing put back. upsert_record must never call
    _replace_purchase_entry_lines when new_lines is empty.
    """

    def test_empty_lines_on_update_skips_replace_entirely(self):
        sink = make_sink([FakeResponse(204)])  # header PUT only
        record = {
            "Id": "entry-1",
            "Currency": "EUR",
            "PurchaseEntryLines": [],
        }

        id_, success, state_updates = sink.upsert_record(record, {})

        assert id_ == "entry-1"
        assert success is True
        # only the header PUT should have been sent - no GET/POST/DELETE for lines
        assert sink.request_api.call_count == 1
        assert sink.request_api.call_args_list[0].args[0] == "PUT"
        sink.logger.warning.assert_called_once()

    def test_none_lines_on_update_skips_replace_without_warning(self):
        sink = make_sink([FakeResponse(204)])
        record = {"Id": "entry-1", "Currency": "EUR"}

        sink.upsert_record(record, {})

        assert sink.request_api.call_count == 1
        sink.logger.warning.assert_not_called()

    def test_non_empty_lines_on_update_still_replaces(self):
        sink = make_sink(
            [
                FakeResponse(204),  # header PUT
                FakeResponse(200),  # GET existing lines
                FakeResponse(201),  # POST new line
                FakeResponse(204),  # DELETE old line
            ]
        )
        record = {
            "Id": "entry-1",
            "Currency": "EUR",
            "PurchaseEntryLines": [{"AmountFC": 100}],
        }

        with patch(
            "target_exact.sinks.xmltodict.parse",
            side_effect=[entry_ids_feed(["old-1"]), created_entry("new-1")],
        ):
            sink.upsert_record(record, {})

        methods = [c.args[0] for c in sink.request_api.call_args_list]
        assert methods == ["PUT", "GET", "POST", "DELETE"]
        sink.logger.warning.assert_not_called()
