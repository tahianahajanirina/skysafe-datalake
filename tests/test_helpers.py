"""Tests for utility functions in helpers.py."""
import pytest
from helpers import _parse_s3_uri, _s3_uri, join_path


class TestParseS3Uri:
    def test_parses_s3a_uri(self):
        bucket, key = _parse_s3_uri("s3a://my-bucket/path/to/file.json")
        assert bucket == "my-bucket"
        assert key == "path/to/file.json"

    def test_parses_s3_uri(self):
        bucket, key = _parse_s3_uri("s3://my-bucket/key")
        assert bucket == "my-bucket"
        assert key == "key"

    def test_bucket_only(self):
        bucket, key = _parse_s3_uri("s3a://my-bucket")
        assert bucket == "my-bucket"
        assert key == ""


class TestS3Uri:
    def test_builds_uri(self):
        uri = _s3_uri("raw/opensky/flights")
        assert uri.startswith("s3a://")
        assert "raw/opensky/flights" in uri


class TestJoinPath:
    def test_joins_s3_paths(self):
        result = join_path("s3a://bucket/base", "sub", "file.json")
        assert result == "s3a://bucket/base/sub/file.json"

    def test_joins_s3_paths_strips_slashes(self):
        result = join_path("s3a://bucket/base/", "/sub/", "/file.json")
        assert result == "s3a://bucket/base/sub/file.json"

    def test_joins_local_paths(self):
        result = join_path("/tmp/data", "sub", "file.json")
        assert result == "/tmp/data/sub/file.json"
