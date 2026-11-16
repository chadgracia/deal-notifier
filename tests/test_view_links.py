"""Deal-gate personal view links (?v=) in deal alert and digest emails.
No AWS calls, no emails: every S3/SES/Pipeline touchpoint is mocked. Fake secrets only."""
import hashlib
import hmac
import os
import re
import sys
import time
import types
import unittest
from datetime import datetime, timezone
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

VIEW_SECRET = "test-view-secret"
SITE = "https://trades.graciagroup.com"


# --- verbatim copy of the deal gate's verifier (chadgracia/trades dg_verify_token) ---
_DG_TOKEN_RE = re.compile(r"^([A-Za-z0-9_-]{1,64})\.(\d{1,12})\.([0-9a-f]{32})$")
DG_LINK_TTL = 183 * 86400
DG_TOKEN_SKEW = 300


def _dg_sig(prefix, pid, exp, secret):
    return hmac.new(secret.encode(), f"{prefix}|{pid}|{exp}".encode(),
                    hashlib.sha256).hexdigest()[:32]


def dg_verify_token(token, prefix="view1", now=None, secret=VIEW_SECRET):
    if not secret or not isinstance(token, str):
        return None
    m = _DG_TOKEN_RE.match(token)
    if not m:
        return None
    pid, exp, sig = m.group(1), m.group(2), m.group(3)
    now_i = int(time.time() if now is None else now)
    if int(exp) <= now_i:
        return None
    if int(exp) - now_i > DG_LINK_TTL + DG_TOKEN_SKEW:
        return None
    if not hmac.compare_digest(_dg_sig(prefix, pid, exp, secret), sig):
        return None
    return pid
# --- end copy ---


def recent_ts():
    return datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S +0000")


DEAL = {"id": 555, "name": "Acme lot", "deal_stage": {"id": lf.FIRM_STAGE_ID}, "company": {"name": "Acme"},
        "updated_at": None,
        "custom_fields": {lf.DEAL_TYPE_FIELD: [lf.SELL_TYPE_ID], lf.NEXUS_FIELD: [lf.NEXUS_DIRECT_ID],
                          lf.GROSS_FIELD: 10}}


def person(pid, email, first, **cf):
    c = {lf.BUYING_FIELD: [77]}
    c.update(cf)
    return {"id": pid, "email": email, "first_name": first, "last_name": "X",
            "predefined_contacts_tag_ids": [lf.WHITELIST_TAG_ID], "custom_fields": c}


PEOPLE = [
    person(9001, "ann@client.com", "Ann"),
    person(9002, "bob@client.com", "Bob"),
    person(9003, "rob@rainmakersecurities.com", "Rob"),                       # Rainmaker
    person(9004, "amy@sg.rainmakerapac.com", "Amy"),                          # subdomain
    person(9005, "STaylor@spry.vc", "Sta"),                                   # listed broker
    person(9006, "cob@client.com", "Cob", **{"custom_label_3759163": [6577160]}),
    person(9007, "ffi@client.com", "Ffi", **{"custom_label_3759163": 6888332}),
    person(9008, "int@client.com", "Int", **{"custom_label_3759163": "6888333"}),
    dict(person(9009, "dee@client.com", "Dee"), home_email="dee@rainmakersecurities.com"),  # 2nd address
    person(9010, "nat@client.com", "Nat", **{"custom_label_3759163": [6484810]}),            # natural person: fine
]
OK_IDS = {9001, 9002, 9010}


class Harness:
    """Runs the manual alert or the daily digest with everything external mocked."""

    def __init__(self, secret, dry_run=False):
        self.secret, self.dry_run, self.sent = secret, dry_run, []

    def _send(self, to, subject, body, html=None):
        self.sent.append({"to": to, "subject": subject, "body": body, "html": html})

    def patches(self):
        deal = dict(DEAL, updated_at=recent_ts())
        snaps = {"deals.json": {"deals": [deal]}, "people.json": {"people": PEOPLE}}
        return [
            mock.patch.dict(os.environ, {"VIEW_LINK_SECRET": self.secret}),
            mock.patch.object(lf, "DRY_RUN", self.dry_run),
            mock.patch.object(lf, "MAX_EMAILS", 100),
            mock.patch.object(lf, "send_email", self._send),
            mock.patch.object(lf, "get_jwt", lambda: "test-jwt"),
            mock.patch.object(lf, "load_snapshot", lambda k: snaps[k]),
            mock.patch.object(lf, "load_field_entries",
                              lambda fid, jwt: {77: "Acme"} if fid == 3322093 else {}),
            mock.patch.object(lf, "load_live_auctions", lambda: []),
            mock.patch.object(lf, "load_visibility_overrides", lambda: {}),
            mock.patch.object(lf.time, "sleep", lambda s: None),
            mock.patch.object(lf, "admin_key_ok", lambda k: True),
            mock.patch.object(lf, "fetch_live_deal", lambda d, j: deal),
            mock.patch.object(lf, "parse_post_body", lambda e: {"key": "test-key", "deal_id": "555"}),
            mock.patch.object(lf, "find_matches_for_deal", lambda *a: list(PEOPLE)),
            mock.patch.object(lf, "load_alert_history", lambda: {}),
            mock.patch.object(lf, "save_alert_history", lambda h: None),
            mock.patch.object(lf, "boto3", types.SimpleNamespace(client=lambda *a, **k: mock.MagicMock())),
        ]

    def run(self, which):
        ps = self.patches()
        for p in ps:
            p.start()
        try:
            with unittest.TestCase().assertLogs(level="INFO") as logs:
                if which == "alert":
                    lf.handle_alert_post({})
                else:
                    lf.run_digest({}, None)
            self.logs = "\n".join(logs.output)
        finally:
            for p in reversed(ps):
                p.stop()
        return self.sent


def personal(sent):
    return [m for m in sent if "[ARCHIVE]" not in m["subject"]]


def tokens_in(text):
    return re.findall(r"/deal/555(?:\?|&amp;|&)v=([A-Za-z0-9_.-]+)", text or "")


class TokenFormat(unittest.TestCase):
    def test_matches_deal_gate(self):
        with mock.patch.dict(os.environ, {"VIEW_LINK_SECRET": VIEW_SECRET}):
            tok = lf.make_view_token(9001, now=1_900_000_000)
        pid, exp, sig = tok.split(".")
        self.assertEqual((pid, int(exp)), ("9001", 1_900_000_000 + 183 * 86400))
        self.assertEqual(sig, _dg_sig("view1", "9001", exp, VIEW_SECRET))
        self.assertEqual(dg_verify_token(tok, now=1_900_000_000 + 100), "9001")
        self.assertIsNone(dg_verify_token(tok, now=1_900_000_000 + 184 * 86400))   # expired
        self.assertIsNone(dg_verify_token(tok, prefix="viewc1", now=1_900_000_000))  # link != cookie
        self.assertIsNone(dg_verify_token(tok, now=1_900_000_000, secret="other"))

    def test_no_secret_no_token(self):
        with mock.patch.dict(os.environ, {"VIEW_LINK_SECRET": ""}):
            self.assertIsNone(lf.make_view_token(9001))
            self.assertIsNone(lf.recipient_view_token(PEOPLE[0], dry_run=False))

    def test_add_view_token(self):
        t = "9001.1.abc"
        self.assertEqual(lf.add_view_token(f"See {SITE}/deal/1 now", t), f"See {SITE}/deal/1?v={t} now")
        self.assertEqual(lf.add_view_token(f"<a href='{SITE}/deal/2'>", t), f"<a href='{SITE}/deal/2?v={t}'>")
        self.assertEqual(lf.add_view_token(f'<a href="{SITE}/deal/3?x=1">', t), f'<a href="{SITE}/deal/3?x=1&v={t}">')
        self.assertEqual(lf.add_view_token(f"{SITE}/deal/4?v=old", t), f"{SITE}/deal/4?v=old")
        for other in ("https://desk.graciagroup.com/deal/5", f"{SITE}/?company=Acme", "http://trades.graciagroup.com/deal/6"):
            self.assertEqual(lf.add_view_token(other, t), other)
        self.assertEqual(lf.add_view_token(f"{SITE}/deal/7", None), f"{SITE}/deal/7")

    def test_hard_block_rules(self):
        blocked = {p["id"] for p in PEOPLE if lf.person_hard_blocked(p)}
        self.assertEqual(blocked, {9003, 9004, 9005, 9006, 9007, 9008, 9009})
        self.assertEqual(len(lf.DG_HARD_EMAILS), 22)


class Emails(unittest.TestCase):
    def check_live(self, which):
        h = Harness(VIEW_SECRET)
        sent = h.run(which)
        mails = personal(sent)
        by_to = {m["to"]: m for m in mails}
        seen = set()
        for p in PEOPLE:
            if which == "digest" and lf.alert_excluded(p):
                self.assertNotIn(p["email"], by_to)                          # Rainmaker / listed: no digest
                continue
            m = by_to[p["email"]]
            text_toks, html_toks = tokens_in(m["body"]), tokens_in(m["html"])
            if p["id"] in OK_IDS:
                self.assertTrue(text_toks and html_toks, p["email"])           # text line AND button
                for tok in text_toks + html_toks:
                    self.assertEqual(dg_verify_token(tok), str(p["id"]))
                seen.update(text_toks + html_toks)
            else:
                self.assertEqual(text_toks + html_toks, [], p["email"])
                self.assertIn(f"{SITE}/deal/555", m["body"])                  # plain link kept
        self.assertEqual(len({dg_verify_token(t) for t in seen}), len(OK_IDS))  # one per recipient
        for t in seen:
            self.assertNotIn(t, h.logs)
            self.assertNotIn(t.split(".")[2], h.logs)
        self.assertNotIn(VIEW_SECRET, h.logs)
        return sent

    def test_alert_per_recipient_tokens_and_clean_archive(self):
        sent = self.check_live("alert")
        m = next(x for x in sent if "View details:" in x["body"] and x["to"] == "ann@client.com")
        self.assertRegex(m["body"], r"View details: https://trades\.graciagroup\.com/deal/555\?v=9001\.\d+\.[0-9a-f]{32}\n")
        self.assertRegex(m["html"], r"href='https://trades\.graciagroup\.com/deal/555\?v=9001\.\d+\.[0-9a-f]{32}'")
        arch = [x for x in sent if "[ARCHIVE]" in x["subject"]]
        self.assertEqual(len(arch), 1)
        self.assertIn(f"{SITE}/deal/555", arch[0]["body"])
        for part in (arch[0]["body"], arch[0]["html"]):
            self.assertNotRegex(part, r"[?&]v=|&amp;v=")

    def test_digest_per_recipient_tokens(self):
        self.check_live("digest")

    def test_dry_run_uses_chads_token(self):
        for which in ("alert", "digest"):
            sent = Harness(VIEW_SECRET, dry_run=True).run(which)
            for m in personal(sent):
                self.assertEqual(m["to"], lf.CHAD_EMAIL)
                toks = tokens_in(m["body"]) + tokens_in(m["html"])
                self.assertTrue(toks)
                self.assertEqual({dg_verify_token(t) for t in toks}, {"1259927678"})

    def test_empty_secret_is_todays_output(self):
        for which in ("alert", "digest"):
            h = Harness("")
            sent = h.run(which)
            self.assertEqual(h.logs.count("view links off (VIEW_LINK_SECRET not set)"), 1)
            for m in sent:
                for part in (m["body"], m["html"]):
                    self.assertNotRegex(part or "", r"/deal/555(\?|&amp;|&)v=")
            # identical to the same run with the link step removed entirely
            with mock.patch.object(lf, "add_view_token", lambda c, t: c), \
                    mock.patch.object(lf, "recipient_view_token", lambda *a, **k: None):
                base = Harness("").run(which)
            self.assertEqual(sent, base)


if __name__ == "__main__":
    unittest.main()
