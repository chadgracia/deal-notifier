"""deal-notifier mints interest-form links with HMAC_SECRET only; an empty key
refuses the alert and skips the digest. Fake keys; no AWS, Pipeline or email."""
import base64
import hashlib
import hmac
import os
import sys
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("LOOKBACK_HOURS", "24")
os.environ.setdefault("HMAC_SECRET", "test-key")
os.environ.setdefault("PORTFOLIO_HMAC_SECRET", "test-key")
os.environ.setdefault("ADMIN_KEY", "test-key")
os.environ["DRY_RUN"] = "true"
if "boto3" not in sys.modules:
    _stub = types.ModuleType("boto3")
    def _no_aws(*a, **k):
        raise AssertionError("tests must not create real AWS clients")
    _stub.client = _no_aws
    sys.modules["boto3"] = _stub
import lambda_function as lf  # noqa: E402


def sign(key, v):
    return base64.urlsafe_b64encode(hmac.new(key.encode(), str(v).encode(), hashlib.sha256).digest()).decode().rstrip("=")


class Rotation(unittest.TestCase):
    def test_token_uses_current_key_only(self):
        with mock.patch.object(lf, "HMAC_SECRET", "test-new-hmac"):
            self.assertEqual(lf.make_token(7), sign("test-new-hmac", 7))
            self.assertNotEqual(lf.make_token(7), sign("test-old-hmac", 7))

    def test_empty_key_alert_refused(self):
        with mock.patch.object(lf, "HMAC_SECRET", ""), \
                mock.patch.object(lf, "parse_post_body", return_value={"key": "test-key", "deal_id": "9"}), \
                mock.patch.object(lf, "admin_key_ok", return_value=True), \
                mock.patch.object(lf, "get_jwt", side_effect=AssertionError("no Pipeline")), \
                mock.patch.object(lf, "send_email", side_effect=AssertionError("no email")):
            self.assertEqual(lf.make_token(7), "")
            r = lf.handle_alert_post({})
        self.assertIn("Nothing sent", r["body"])

    def test_empty_key_digest_skipped(self):
        with mock.patch.object(lf, "HMAC_SECRET", ""), \
                mock.patch.object(lf, "get_jwt", side_effect=AssertionError("no Pipeline")), \
                mock.patch.object(lf, "send_email", side_effect=AssertionError("no email")):
            r = lf.run_digest({}, None)
        self.assertEqual(r["statusCode"], 503)


if __name__ == "__main__":
    unittest.main()
