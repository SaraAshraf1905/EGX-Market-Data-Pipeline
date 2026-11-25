"""Unit tests for src/messages.py (no Kafka / MongoDB / internet needed).

Run:  python -m unittest discover -s tests -v
"""
import json
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import messages  # noqa: E402

CAIRO = timezone(timedelta(hours=2))
ROW = {"Open": 95.1, "High": 95.4, "Low": 94.9, "Close": 95.3, "Volume": 120450.0}


class MessageTests(unittest.TestCase):
    def make(self, **overrides):
        row = {**ROW, **overrides}
        return messages.build_message(" comi.ca ", datetime(2026, 11, 15, 10, 5, tzinfo=CAIRO),
                                      row, interval="5m", fetched_at="2026-11-15T08:07:12Z")

    def test_symbol_is_cleaned_and_times_are_converted(self):
        m = self.make()
        self.assertEqual(m["symbol"], "COMI.CA")
        self.assertEqual(m["bar_time"], "2026-11-15T10:05:00+02:00")
        self.assertEqual(m["bar_time_utc"], "2026-11-15T08:05:00Z")   # Cairo UTC+2 -> UTC

    def test_naive_datetime_is_rejected(self):
        with self.assertRaises(ValueError):
            messages.build_message("X", datetime(2026, 1, 1), ROW, interval="5m")

    def test_nan_and_missing_values_become_none(self):
        m = self.make(Volume=float("nan"), High=None)
        self.assertIsNone(m["volume"])
        self.assertIsNone(m["high"])

    def test_volume_is_integer_and_prices_are_decimal(self):
        m = self.make()
        self.assertIsInstance(m["volume"], int)
        self.assertEqual(m["volume"], 120450)
        self.assertIsInstance(m["close"], float)

    def test_valid_message_has_no_problems(self):
        self.assertEqual(messages.validate_message(self.make()), [])

    def test_missing_close_is_reported(self):
        m = self.make(Close=None)
        self.assertIn("missing field: close", messages.validate_message(m))

    def test_non_object_is_reported(self):
        self.assertEqual(messages.validate_message([1, 2]), ["message is not a JSON object"])

    def test_doc_id_is_stable_so_duplicates_overwrite(self):
        self.assertEqual(messages.make_doc_id(self.make()), messages.make_doc_id(self.make(Close=96.0)))
        self.assertEqual(messages.make_doc_id(self.make()), "COMI.CA|5m|2026-11-15T08:05:00Z")

    def test_encode_decode_roundtrip(self):
        m = self.make()
        self.assertEqual(messages.decode(messages.encode(m)), m)
        self.assertEqual(json.loads(messages.encode(m))["symbol"], "COMI.CA")

    def test_decode_rejects_garbage(self):
        with self.assertRaises(ValueError):
            messages.decode(b"this is not json")
        with self.assertRaises(ValueError):
            messages.decode(b"\xff\xfe")


if __name__ == "__main__":
    unittest.main()
