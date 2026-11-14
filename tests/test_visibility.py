"""Deal Visibility tests. No AWS calls, no emails: boto3 is stubbed and every
S3/SES/Pipeline touchpoint is mocked."""
import io
import json
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

VF = lf.VISIBILITY_FIELD


class NoSuchKey(Exception):
    pass


def fake_s3(objects):
    """S3 client stub serving JSON objects by key; records puts."""
    s3 = mock.MagicMock()
    def get_object(Bucket, Key):
        if Key not in objects:
            raise NoSuchKey("An error occurred (NoSuchKey)")
        return {"Body": io.BytesIO(json.dumps(objects[Key]).encode())}
    s3.get_object.side_effect = get_object
    return s3


def deal(did, vis=None, company="Acme"):
    cf = {}
    if vis is not None:
        cf[VF] = vis
    return {"id": did, "custom_fields": cf, "company": {"name": company},
            "deal_stage": {"id": lf.FIRM_STAGE_ID}, "is_archived": False,
            "updated_at": "2026/11/14 10:00:00 +0000"}


def reset_cache():
    lf._vis_overrides_cache["at"] = 0.0
    lf._vis_overrides_cache["data"] = None


class NormalizeTests(unittest.TestCase):
    def test_values(self):
        n = lf.normalize_visibility
        for v in (None, "", [], "abc", True, 12345, {"x": 1}, "  "):
            self.assertEqual(n(v), lf.VIS_ALL, v)
        self.assertEqual(n(lf.VIS_PRIVATE), lf.VIS_PRIVATE)
        self.assertEqual(n("7223458"), lf.VIS_PRIVATE)
        self.assertEqual(n("7223458.0"), lf.VIS_PRIVATE)
        self.assertEqual(n([7223458]), lf.VIS_PRIVATE)
        self.assertEqual(n(["7223457"]), lf.VIS_INTERESTED)
        self.assertEqual(n(7223456), lf.VIS_ALL)


class LookupTests(unittest.TestCase):
    def setUp(self):
        reset_cache()

    def test_override_wins_and_cache(self):
        s3 = fake_s3({"visibility-overrides.json":
                      {"5": {"v": lf.VIS_PRIVATE, "at": "2026-11-14T10:00:00Z"},
                       "6": {"v": "7223456", "at": "2026-11-14T10:00:00Z"}}})
        with mock.patch.object(lf.boto3, "client", return_value=s3):
            self.assertTrue(lf.deal_is_private(deal(5)))                  # blank field, override Private
            self.assertFalse(lf.deal_is_private(deal(6, lf.VIS_PRIVATE)))  # field Private, override All
            self.assertEqual(lf.deal_visibility(deal(7, lf.VIS_INTERESTED)), lf.VIS_INTERESTED)
            self.assertEqual(lf.deal_visibility(deal(8)), lf.VIS_ALL)
        self.assertEqual(s3.get_object.call_count, 1)  # 60s cache

    def test_missing_file_is_empty(self):
        with mock.patch.object(lf.boto3, "client", return_value=fake_s3({})):
            self.assertEqual(lf.load_visibility_overrides(), {})
            self.assertTrue(lf.deal_is_private(deal(1, [lf.VIS_PRIVATE])))

    def test_lookup_error_fails_soft(self):
        bad = mock.MagicMock()
        bad.get_object.side_effect = RuntimeError("boom")
        with mock.patch.object(lf.boto3, "client", return_value=bad), \
             self.assertLogs(lf.logger, "WARNING") as logs:
            self.assertEqual(lf.load_visibility_overrides(), {})
            self.assertEqual(lf.deal_visibility(deal(1)), lf.VIS_ALL)
            self.assertTrue(lf.deal_is_private(deal(2, lf.VIS_PRIVATE)))
        self.assertTrue(any("visibility:" in m for m in logs.output))


def post_event(deal_id):
    return {"requestContext": {"http": {"method": "POST"}},
            "body": f"key=test-key&deal_id={deal_id}", "isBase64Encoded": False}


class AlertPostTests(unittest.TestCase):
    def setUp(self):
        reset_cache()
        self.p = [
            mock.patch.object(lf, "ADMIN_KEY", "test-key"),
            mock.patch.object(lf, "get_jwt", return_value="test-jwt"),
            mock.patch.object(lf, "load_snapshot", return_value={"people": []}),
            mock.patch.object(lf, "load_field_entries", return_value={}),
            mock.patch.object(lf, "find_matches_for_deal", return_value=[
                {"id": 1, "email": "x@example.com", "first_name": "X"}]),
            mock.patch.object(lf, "send_email"),
            mock.patch.object(lf, "load_alert_history", return_value={}),
            mock.patch.object(lf, "save_alert_history"),
            mock.patch.object(lf, "send_alert_archive"),
        ]
        for p in self.p:
            p.start()
        self.addCleanup(mock.patch.stopall)

    def run_post(self, d, overrides=None):
        objs = {"visibility-overrides.json": overrides} if overrides is not None else {}
        s3 = fake_s3(objs)
        with mock.patch.object(lf, "fetch_live_deal", return_value=d), \
             mock.patch.object(lf.boto3, "client", return_value=s3):
            return lf.lambda_handler(post_event(d["id"]), None)

    def test_private_refused(self):
        res = self.run_post(deal(10, "7223458"))
        self.assertIn("Deal is Private. Nothing sent.", res["body"])
        lf.send_email.assert_not_called()
        lf.find_matches_for_deal.assert_not_called()

    def test_private_override_refused(self):
        res = self.run_post(deal(11), {"11": {"v": lf.VIS_PRIVATE, "at": "2026-11-14T10:00:00Z"}})
        self.assertIn("Deal is Private. Nothing sent.", res["body"])
        lf.send_email.assert_not_called()

    def test_all_interested_blank_send(self):
        for vis in (None, lf.VIS_ALL, lf.VIS_INTERESTED):
            reset_cache()
            lf.send_email.reset_mock()
            res = self.run_post(deal(12, vis))
            self.assertIn("Alert report", res["body"], vis)
            self.assertEqual(lf.send_email.call_count, 1, vis)
            # DRY_RUN: only ever routed to Chad
            self.assertEqual(lf.send_email.call_args[0][0], lf.CHAD_EMAIL)


class AdminViewTests(unittest.TestCase):
    def setUp(self):
        reset_cache()
        self.deals = [deal(1, None, "Alpha"), deal(2, lf.VIS_ALL, "Bravo"),
                      deal(3, lf.VIS_INTERESTED, "Charlie"), deal(4, [lf.VIS_PRIVATE], "Delta"),
                      deal(5, None, "Echo")]
        snaps = {"deals.json": {"deals": self.deals}, "people.json": {"people": []}}
        mock.patch.object(lf, "ADMIN_KEY", "test-key").start()
        mock.patch.object(lf, "get_jwt", return_value="test-jwt").start()
        mock.patch.object(lf, "load_snapshot", side_effect=lambda k: snaps[k]).start()
        mock.patch.object(lf, "load_field_entries", return_value={}).start()
        mock.patch.object(lf, "load_alert_history", return_value={}).start()
        mock.patch.object(lf, "load_client_standing", return_value={}).start()
        mock.patch.object(lf, "find_matches_for_deal", return_value=[
            {"id": 9, "first_name": "P"}]).start()
        mock.patch.object(lf.boto3, "client", return_value=fake_s3({
            "visibility-overrides.json": {"5": {"v": lf.VIS_PRIVATE, "at": "x"}}})).start()
        self.addCleanup(mock.patch.stopall)

    def test_admin_table(self):
        html = lf.render_admin_table({"queryStringParameters": {"key": "test-key"}})["body"]
        rows = {r.split("target='_blank'>")[1].split("<")[0]: r
                for r in html.split("<tr data-side=")[1:]}
        for name in ("Alpha", "Bravo", "Charlie"):
            self.assertIn("alertCounterparties(", rows[name], name)
            self.assertNotIn(">Private<", rows[name], name)
        for name in ("Delta", "Echo"):  # field Private; override Private
            self.assertNotIn("alertCounterparties(", rows[name], name)
            self.assertIn(">Private<", rows[name], name)

    def test_note_preview(self):
        def prev(did):
            r = lf.render_note_preview({"queryStringParameters":
                                        {"key": "test-key", "deal_id": str(did)}})
            return json.loads(r["body"])
        for did in (1, 2, 3):
            self.assertEqual(prev(did)["total"], 1, did)
        for did in (4, 5):
            p = prev(did)
            self.assertEqual((p["total"], p["keep"], p["cut"]), (0, [], []))
            self.assertIn("Deal is Private. Nothing sent.", p["warnings"])

    def test_recipients_view(self):
        def view(did):
            return lf.render_recipients_view({"queryStringParameters":
                                              {"key": "test-key", "deal_id": str(did)}})["body"]
        self.assertIn("Deal is Private. Nothing sent.", view(4))
        self.assertIn("Deal is Private. Nothing sent.", view(5))
        self.assertNotIn("Private", view(1))
        self.assertNotIn("Private", view(3))


class DigestTests(unittest.TestCase):
    def test_private_skipped(self):
        reset_cache()
        deals = [deal(1, None, "Alpha"), deal(2, lf.VIS_ALL, "Bravo"),
                 deal(3, lf.VIS_INTERESTED, "Charlie"), deal(4, "7223458.0", "Delta"),
                 deal(5, None, "Echo")]
        for d in deals:
            d["custom_fields"][lf.NEXUS_FIELD] = [lf.NEXUS_DIRECT_ID]
            d["custom_fields"][lf.DEAL_TYPE_FIELD] = lf.SELL_TYPE_ID
            d["updated_at"] = lf.datetime.now(lf.timezone.utc).strftime("%Y/%m/%d %H:%M:%S +0000")
        snaps = {"deals.json": {"deals": deals}, "people.json": {"people": []}}
        logged = []
        with mock.patch.object(lf, "get_jwt", return_value="test-jwt"), \
             mock.patch.object(lf, "load_snapshot", side_effect=lambda k: snaps[k]), \
             mock.patch.object(lf, "load_field_entries", return_value={}), \
             mock.patch.object(lf, "load_live_auctions", return_value=set()), \
             mock.patch.object(lf, "send_email") as se, \
             mock.patch.object(lf.logger, "info", side_effect=lambda m, *a: logged.append(m)), \
             mock.patch.object(lf.boto3, "client", return_value=fake_s3({
                 "visibility-overrides.json": {"5": {"v": lf.VIS_PRIVATE, "at": "x"}}})):
            try:
                lf.run_digest({}, None)
            except Exception:
                pass
            se.assert_not_called()
        line = next(m for m in logged if m.startswith("Sell companies:"))
        for name in ("alpha", "bravo", "charlie"):
            self.assertIn(name, line)
        for name in ("delta", "echo"):
            self.assertNotIn(name, line)


if __name__ == "__main__":
    unittest.main()
