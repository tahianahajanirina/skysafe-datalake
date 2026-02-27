"""Tests for pure-Python helpers in format_flights.py."""
import pytest
from format_flights import _safe_get, _to_float, _clean_callsign


class TestSafeGet:
    def test_returns_element_at_index(self):
        assert _safe_get(["a", "b", "c"], 1) == "b"

    def test_returns_none_when_index_out_of_range(self):
        assert _safe_get(["a"], 5) is None

    def test_returns_none_for_non_list(self):
        assert _safe_get(None, 0) is None
        assert _safe_get("string", 0) is None

    def test_returns_none_value_in_list(self):
        assert _safe_get([None, "x"], 0) is None


class TestToFloat:
    def test_converts_int(self):
        assert _to_float(10) == 10.0

    def test_converts_string_number(self):
        assert _to_float("3.14") == pytest.approx(3.14)

    def test_returns_none_for_none(self):
        assert _to_float(None) is None

    def test_returns_none_for_non_numeric_string(self):
        assert _to_float("abc") is None

    def test_handles_zero(self):
        assert _to_float(0) == 0.0


class TestCleanCallsign:
    def test_strips_whitespace(self):
        assert _clean_callsign("  AFR123  ") == "AFR123"

    def test_returns_none_for_empty_string(self):
        assert _clean_callsign("   ") is None

    def test_returns_none_for_none(self):
        assert _clean_callsign(None) is None

    def test_preserves_valid_callsign(self):
        assert _clean_callsign("BAW456") == "BAW456"
