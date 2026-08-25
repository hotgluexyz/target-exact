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


def make_sink(request_side_effect, config=None):
    """Build a PurchaseEntriesSink without running HotglueSink.__init__."""
    sink = PurchaseEntriesSink.__new__(PurchaseEntriesSink)
    sink.request_api = MagicMock(side_effect=request_side_effect)
    sink.logger = MagicMock()
    sink.name = "PurchaseEntries"
    sink.endpoint = "/purchaseentry/PurchaseEntries"
    sink._config = config or {}
    return sink


def entry_ids_feed(ids, link=None):
    """Mimic xmltodict output for a GET on PurchaseEntryLines.

    `link` mimics the Atom <link> element(s) xmltodict would produce - a single
    dict when there's exactly one, a list when there's more than one (e.g. a
    "self" link alongside a "next" pagination link).
    """
    if not ids and link is None:
        return {"feed": None}
    entries = [{"content": {"m:properties": {"d:ID": {"#text": i}}}} for i in ids]
    feed = {}
    if entries:
        feed["entry"] = entries[0] if len(entries) == 1 else entries
    if link is not None:
        feed["link"] = link
    return {"feed": feed}


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

    def test_follows_next_link_across_pages(self):
        """Exact caps PurchaseEntryLines feeds at a page size; a `next` link means
        there's another page of existing lines still to fetch. Missing it would
        leave those lines out of the delete step in _replace_purchase_entry_lines,
        stranding stale lines alongside the new ones."""
        sink = make_sink([FakeResponse(200), FakeResponse(200)])
        next_href = (
            "https://start.exactonline.nl/api/v1/123456/purchaseentry/PurchaseEntryLines"
            "?$filter=EntryID+eq+guid%27entry-1%27&$select=ID&$skiptoken=guid%27old-1%27"
        )
        with patch(
            "target_exact.sinks.xmltodict.parse",
            side_effect=[
                entry_ids_feed(["old-1"], link={"@rel": "next", "@href": next_href}),
                entry_ids_feed(["old-2"]),
            ],
        ):
            ids = sink._get_existing_line_ids("entry-1")

        assert ids == ["old-1", "old-2"]
        assert sink.request_api.call_count == 2
        calls = sink.request_api.call_args_list
        assert calls[0].args[0] == "GET"
        assert calls[0].kwargs["endpoint"] == "/purchaseentry/PurchaseEntryLines"
        # the second request must be re-issued against the same relative endpoint
        # (request_api always prepends base_url) using the params pulled out of
        # the absolute next-page href, not the href itself
        assert calls[1].kwargs["endpoint"] == "/purchaseentry/PurchaseEntryLines"
        assert calls[1].kwargs["params"] == {
            "$filter": "EntryID eq guid'entry-1'",
            "$select": "ID",
            "$skiptoken": "guid'old-1'",
        }

    def test_ignores_non_next_links(self):
        """A `self` link (or any non-`next` rel) must not be mistaken for pagination."""
        sink = make_sink([FakeResponse(200)])
        with patch(
            "target_exact.sinks.xmltodict.parse",
            return_value=entry_ids_feed(
                ["old-1"], link={"@rel": "self", "@href": "https://irrelevant/self"}
            ),
        ):
            ids = sink._get_existing_line_ids("entry-1")

        assert ids == ["old-1"]
        assert sink.request_api.call_count == 1

    def test_multiple_link_elements_parsed_as_list(self):
        """Exact's real feed carries both a self and a next <link>; xmltodict parses
        multiple same-name sibling elements as a list, not a single dict - the
        pagination lookup must handle that shape, not just a lone next link."""
        sink = make_sink([FakeResponse(200), FakeResponse(200)])
        next_href = "https://irrelevant/purchaseentry/PurchaseEntryLines?$skiptoken=guid%27old-1%27"
        with patch(
            "target_exact.sinks.xmltodict.parse",
            side_effect=[
                entry_ids_feed(
                    ["old-1"],
                    link=[
                        {"@rel": "self", "@href": "https://irrelevant/self"},
                        {"@rel": "next", "@href": next_href},
                    ],
                ),
                entry_ids_feed(["old-2"]),
            ],
        ):
            ids = sink._get_existing_line_ids("entry-1")

        assert ids == ["old-1", "old-2"]
        assert sink.request_api.call_count == 2


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

    These tests exercise that guard with the line-replacement flag on, since
    that's the only state where _replace_purchase_entry_lines is ever reached
    (see TestReplacePurchaseEntryLinesFlag for the flag-off/default behavior).
    """

    FLAG_ON = {"replace_purchase_entry_lines_on_update": True}

    def test_empty_lines_on_update_skips_replace_entirely(self):
        sink = make_sink([FakeResponse(204)], config=self.FLAG_ON)  # header PUT only
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
        sink = make_sink([FakeResponse(204)], config=self.FLAG_ON)
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
            ],
            config=self.FLAG_ON,
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


class TestReplacePurchaseEntryLinesFlag:
    """Guards the PR review request (code owner keyn4, PR #13): deleting and
    recreating every PurchaseEntryLine is a bigger, riskier change than the
    header-only PUT it replaces, so it must stay opt-in per tenant behind
    replace_purchase_entry_lines_on_update. With the flag unset or false,
    upsert_record must reproduce the pre-fix behavior exactly: header PUT
    only, PurchaseEntryLines changes silently dropped, no warning logged.
    """

    def test_flag_unset_by_default_skips_replace_even_with_new_lines(self):
        sink = make_sink([FakeResponse(204)])  # no config passed -> flag unset
        record = {
            "Id": "entry-1",
            "Currency": "EUR",
            "PurchaseEntryLines": [{"AmountFC": 100}],
        }

        id_, success, state_updates = sink.upsert_record(record, {})

        assert id_ == "entry-1"
        assert success is True
        assert sink.request_api.call_count == 1
        assert sink.request_api.call_args_list[0].args[0] == "PUT"
        sink.logger.warning.assert_not_called()

    def test_flag_explicitly_false_skips_replace(self):
        sink = make_sink(
            [FakeResponse(204)],
            config={"replace_purchase_entry_lines_on_update": False},
        )
        record = {
            "Id": "entry-1",
            "Currency": "EUR",
            "PurchaseEntryLines": [{"AmountFC": 100}],
        }

        sink.upsert_record(record, {})

        assert sink.request_api.call_count == 1
        sink.logger.warning.assert_not_called()
