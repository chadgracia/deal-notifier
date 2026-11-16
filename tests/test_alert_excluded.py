"""alert_excluded: recipient gates shared by manual alerts and the digest. No AWS calls."""
import os
import sys
import types
import unittest

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

SUBSTANTIVE = 7162165
CO_BROKER = 6577160


def person(level=None, status=None, email="investor@example.com", transactor=None, **extra):
    cf = {}
    if level is not None:
        cf[lf.INVESTOR_LEVEL_FIELD] = [level]
    if status is not None:
        cf[lf.EMAIL_STATUS_FIELD] = status
    if transactor is not None:
        cf[lf.DG_TRANSACTOR_FIELD] = [transactor]
    p = {"id": 1, "email": email, "custom_fields": cf}
    p.update(extra)
    return p


class AlertExcludedTests(unittest.TestCase):
    def test_non_accredited_excluded(self):
        self.assertTrue(lf.alert_excluded(person(level=lf.NON_ACCREDITED_ID)))

    def test_substantive_blank_status_included(self):
        self.assertFalse(lf.alert_excluded(person(level=SUBSTANTIVE)))
        self.assertFalse(lf.alert_excluded(person(level=SUBSTANTIVE, status="")))

    def test_each_bad_email_status_excluded(self):
        for sid in (3940558, 4943722, 3940678, 7214691, 4781320):
            with self.subTest(status=sid):
                self.assertTrue(lf.alert_excluded(person(level=SUBSTANTIVE, status=sid)))
                self.assertTrue(lf.alert_excluded(person(level=SUBSTANTIVE, status=str(sid))))

    def test_rainmaker_address_in_any_email_field_excluded(self):
        self.assertTrue(lf.alert_excluded(person(email="someone@rainmakersecurities.com")))
        self.assertTrue(lf.alert_excluded(person(email2="Someone@RainmakerSecurities.com")))
        self.assertTrue(lf.alert_excluded(person(home_email="x@rainmakersecurities.com")))
        self.assertTrue(lf.alert_excluded(person(emails=[{"email": "x@rainmakersecurities.com"}])))

    def test_co_broker_good_address_included(self):
        p = person(level=SUBSTANTIVE, transactor=CO_BROKER)
        self.assertTrue(lf.person_hard_blocked(p))      # still no view token
        self.assertFalse(lf.alert_excluded(p))          # but still alerted

    def test_missing_custom_fields(self):
        self.assertFalse(lf.alert_excluded({"email": "a@example.com", "custom_fields": None}))


if __name__ == "__main__":
    unittest.main()
