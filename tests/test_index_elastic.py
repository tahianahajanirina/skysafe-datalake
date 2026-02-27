"""Tests for _row_to_es_doc transformation logic in index_elastic.py."""
from datetime import datetime
from index_elastic import _row_to_es_doc, INDEX_MAPPING


class TestRowToEsDoc:
    def test_merges_lat_lon_into_geo_point(self):
        doc = _row_to_es_doc({"icao24": "abc123", "latitude": 48.85, "longitude": 2.35})
        assert "location" in doc
        assert doc["location"] == {"lat": 48.85, "lon": 2.35}
        assert "latitude" not in doc
        assert "longitude" not in doc

    def test_skips_location_when_coords_are_none(self):
        doc = _row_to_es_doc({"icao24": "abc123", "latitude": None, "longitude": None})
        assert "location" not in doc

    def test_converts_datetime_to_iso(self):
        ts = datetime(2026, 2, 27, 12, 0, 0)
        doc = _row_to_es_doc({"observation_time": ts, "extracted_at": ts})
        assert doc["observation_time"] == ts.isoformat()
        assert doc["extracted_at"] == ts.isoformat()

    def test_leaves_string_timestamps_unchanged(self):
        doc = _row_to_es_doc({"observation_time": "not-a-datetime"})
        assert doc["observation_time"] == "not-a-datetime"

    def test_original_dict_is_not_mutated(self):
        original = {"latitude": 48.0, "longitude": 2.0, "icao24": "x"}
        _row_to_es_doc(original)
        assert "latitude" in original

    def test_preserves_other_fields(self):
        doc = _row_to_es_doc({"icao24": "ID1", "risk_score": 42, "callsign": "AFR001"})
        assert doc["icao24"] == "ID1"
        assert doc["risk_score"] == 42
        assert doc["callsign"] == "AFR001"


class TestIndexMapping:
    def test_location_is_geo_point(self):
        props = INDEX_MAPPING["mappings"]["properties"]
        assert props["location"]["type"] == "geo_point"

    def test_required_fields_present(self):
        props = INDEX_MAPPING["mappings"]["properties"]
        for field in ("icao24", "callsign", "risk_score", "risk_category", "is_anomaly"):
            assert field in props, f"Missing field: {field}"
