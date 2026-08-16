"""
deal_notifier.py
────────────────
Daily digest Lambda: emails each person a summary of active deals
matching their buy/sell interests and ticket size.

Data sources (S3 full-pipeline-cache):
  - people.json  → person records with email, Buying/Selling interests, ticket size
  - deals.json   → active deals with company, structure, min/max size

Person interest fields:
  - custom_label_3322093 = "Buying"      (multi-select, security entry IDs)
  - custom_label_3759156 = "Selling"     (multi-select, security entry IDs)
  - custom_label_3052210 = "Ticket Size" (multi-select, size tier entry IDs)

Ticket size filter:
  - If person has no ticket size → show all deals
  - Skip deal if deal MIN_SIZE > person max ticket (too large)
  - Skip deal if deal MAX_SIZE < person min ticket (too small)
  - If deal has no size fields → always show

Environment variables:
  DRY_RUN    - "true" (default) sends all emails to Chad only
  MAX_EMAILS - max emails per run, default 10 (applies always)
"""

import json
import logging
import time
import urllib.request
import urllib.error
import os
import hmac
import hashlib
import base64
import boto3
from html import escape as h_escape
from urllib.parse import parse_qs
from datetime import date, datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config ────────────────────────────────────────────────────────────────────
SNAPSHOT_BUCKET     = "full-pipeline-cache"
JWT_BUCKET          = "pipeline-token"
JWT_KEY             = "pipeline-jwt.json"
SES_SENDER          = "agent@agent.graciagroup.com"
CHAD_EMAIL          = "cgracia@rainmakersecurities.com"
TRADES_URL          = "https://trades.graciagroup.com"
# CloudFront front door for the client-facing functions; routes on path prefix,
# so the trailing slash on each prefix is significant.
DESK_URL            = "https://desk.graciagroup.com"
INTEREST_FORM_URL   = "https://mrp5bv4iia7jxjfrvn67tpycsu0jqvny.lambda-url.us-east-1.on.aws/"

DRY_RUN             = os.environ.get("DRY_RUN", "true").lower() == "true"
MAX_EMAILS          = int(os.environ.get("MAX_EMAILS", "10"))
LOOKBACK_HOURS      = int(os.environ["LOOKBACK_HOURS"])
HMAC_SECRET         = os.environ["HMAC_SECRET"]
PORTFOLIO_URL         = DESK_URL
PORTFOLIO_HMAC_SECRET = "D8mxQTEKuLfsbRsCuTX5qMAuQtAx8EngAAlTRzmJTmM"

# Person fields
BUYING_FIELD        = "custom_label_3322093"
SELLING_FIELD       = "custom_label_3759156"
TICKET_SIZE_FIELD   = "custom_label_3052210"
BROADCAST_FIELD     = "custom_label_3774841"
BROADCAST_YES_ID    = 6535328
BROADCAST_NO_ID     = 6535329
BROADCAST_HOLD_ID   = 6535330
WHITELIST_TAG_ID    = 3280123

# Ticket size entry ID → (min, max) in dollars
# min = lower bound of range, max = upper bound (None = no upper limit)
TICKET_SIZE_MAP = {
    6870210: (100_000,     250_000),
    6631962: (100_000,     250_000),
    5014552: (251_000,     999_000),
    5014555: (1_000_000,   5_000_000),
    5014558: (5_000_000,   10_000_000),
    5014561: (10_000_000,  25_000_000),
    5014564: (25_000_000,  50_000_000),
    5014567: (50_000_000,  100_000_000),
    5014570: (100_000_000, None),
}

# Deal custom fields
DEAL_TYPE_FIELD     = "custom_label_1958"
SELL_TYPE_ID        = 5011675
BUY_TYPE_ID         = 5077819
MIN_SIZE_FIELD      = "custom_label_3065488"
MAX_SIZE_FIELD      = "custom_label_3064645"
GROSS_FIELD         = "custom_label_3064339"
NET_FIELD           = "custom_label_3064369"
STRUCTURE_FIELD     = "custom_label_3064360"
FUND_STRUCTURE_ID   = 5077906
DIRECT_STRUCTURE_ID = 6250090
FORWARD_STRUCTURE_ID = 5077903
ACCEPTS_FIELD       = "custom_label_3998063"
ACCEPTS_FORWARDS_ID = 7177776
NEXUS_FIELD         = "custom_label_3751449"
NEXUS_DIRECT_ID     = 6460632
NEXUS_NAMES         = {6460632: "Direct", 6460633: "RMS Broker",
                       6460634: "Foreign Finder", 6460635: "Co-Broker"}
LAYERS_FIELD        = "custom_label_3938743"
LAYERS_MAP          = {7000228: "1-Layer", 7000229: "2-Layer", 7000230: "3-Layer"}
SELLER_FEE_FIELD    = "custom_label_3940560"
MGMT_FEE_FIELD      = "custom_label_3940558"
CARRY_FIELD         = "custom_label_3940559"


def fmt_fee(v):
    """10.0 -> '10', 2.5 -> '2.5', blank -> '-'."""
    try:
        return f"{float(v):g}"
    except (TypeError, ValueError):
        return "-"

# Active deal stages
FIRM_STAGE_ID       = 111800
INQUIRY_STAGE_ID    = 2109142
ACTIVE_STAGES       = {FIRM_STAGE_ID, INQUIRY_STAGE_ID}

# TEMPORARY — see indexing loop. Remove together with the noise filter
# block once the backlog on these names clears.
NOISY_COMPANIES     = {"anthropic"}

DISCLOSURE = """--
DISCLOSURE: Rainmaker Securities, LLC ("RMS") is a FINRA registered broker-dealer and SIPC member. Find this broker-dealer and its agents on BrokerCheck. Our relationship summary can be found on the RMS website (https://www.rainmakersecurities.com/crs).

RMS is engaged by its clients to make referrals to buyers or sellers of private securities ("Securities"). If such client closes a Securities transaction with a buyer or seller so referred, RMS is entitled to a success fee from the client. Such success fee may be in the form of cash or in warrants to purchase securities of the client or client's affiliate. RMS or RMS representatives may hold equity in its issuer clients or in the issuers of securities purchased or sold by the parties to a transaction.

This communication is confidential and is addressed only to its intended recipient. This communication does not represent an offer or solicitation to buy or sell Securities. Such an offer must be made via definitive legal documentation by the seller of securities.

Investments in the Securities are speculative and involve a high degree of risk. An investor in the Securities should have little to no need for liquidity in the foreseeable future and have sufficient finances to withstand the loss of the entire investment.

RMS does not recommend the purchase or sale of Securities. Potential buyers or sellers of the Securities should seek professional counsel prior to entering into any transaction.

RISK FACTORS: Investments in the Securities are speculative and involve a high degree of risk. Companies engaging in private placements may be early stage and high risk. You should be able to afford the increased risk of loss with such investments, including the potential of a total loss. An investor in the Securities should have little to no need for liquidity in the foreseeable future. Unlike an investment purchased on a stock exchange, an investment in a private placement is highly illiquid. You will most likely be investing in restricted securities, may have difficulty finding a buyer for the securities when you can resell and, as a result, may need to hold the securities indefinitely. Limited disclosure: Companies engaging in private placements are not required to provide the disclosure that would be required in a registered offering. Potential buyers or sellers of the Securities should seek professional counsel prior to entering into any transaction."""


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_jwt():
    s3  = boto3.client('s3')
    obj = s3.get_object(Bucket=JWT_BUCKET, Key=JWT_KEY)
    return json.loads(obj['Body'].read())['jwt']


def call_pipeline_api(endpoint, jwt):
    url = f"https://api.pipelinecrm.com/api/v3{endpoint}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {jwt}",
        "Content-Type": "application/json"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return {"status": r.status, "data": json.loads(r.read().decode())}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "data": e.read().decode()}
    except Exception as e:
        return {"status": 500, "data": str(e)}


def load_snapshot(key):
    s3  = boto3.client('s3')
    obj = s3.get_object(Bucket=SNAPSHOT_BUCKET, Key=key)
    return json.loads(obj['Body'].read())


def send_email(to_address, subject, body):
    ses = boto3.client("ses", region_name="us-east-1")
    ses.send_email(
        Source=f'"Chad Gracia / Gracia Group" <{SES_SENDER}>',
        Destination={"ToAddresses": [to_address]},
        Message={
            "Subject": {"Data": subject},
            "Body":    {"Text": {"Data": body}}
        },
        ReplyToAddresses=[CHAD_EMAIL],
    )


def make_token(person_id):
    sig = hmac.new(HMAC_SECRET.encode(), str(person_id).encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode().rstrip("=")


def make_portfolio_token(person_id):
    """Magic-link token for the portfolio/auction site (signed with ITS secret)."""
    sig = hmac.new(PORTFOLIO_HMAC_SECRET.encode(), str(person_id).encode(),
                   hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode().rstrip("=")


def _auction_base_name(name):
    """Company name with any parenthetical stripped, normalised:
    '1X (Norwegian HoldCo)' -> '1x'. Lets bare interest labels match holdcos."""
    out, depth = [], 0
    for ch in str(name or ""):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        elif depth == 0:
            out.append(ch)
    return " ".join("".join(out).split()).strip().lower()


def load_live_auctions():
    """Open auctions from auctions.json, each annotated with its current top bid.
    Live = no close date, or close date today or later. Never raises."""
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=SNAPSHOT_BUCKET, Key="auctions.json")
        auctions = (json.loads(obj["Body"].read()) or {}).get("auctions") or {}
    except Exception as e:
        logger.info(f"auctions.json not loaded: {e}")
        return []
    today_str = date.today().isoformat()
    live = []
    for aid, auc in auctions.items():
        close = (auc.get("close_date") or "").strip()
        if close and close < today_str:
            continue
        company = (auc.get("company") or "").strip()
        if not company:
            continue
        top = None
        try:
            bobj = s3.get_object(Bucket=SNAPSHOT_BUCKET,
                                 Key=f"bids/auction_{aid}.json")
            bids = (json.loads(bobj["Body"].read()) or {}).get("bids") or {}
            for b in bids.values():
                try:
                    g = float(str(b.get("gross")).replace(",", ""))
                except (TypeError, ValueError):
                    continue
                if top is None or g > top:
                    top = g
        except Exception:
            pass
        live.append({"id": str(aid), "company": company,
                     "close_date": close, "top": top})
    logger.info(f"Live auctions: {[(a['company'], a['id']) for a in live]}")
    return live


def is_sell_deal(cf):
    type_ids = cf.get(DEAL_TYPE_FIELD, [])
    if isinstance(type_ids, list):
        return SELL_TYPE_ID in type_ids
    return type_ids == SELL_TYPE_ID


def is_buy_deal(cf):
    type_ids = cf.get(DEAL_TYPE_FIELD, [])
    if isinstance(type_ids, list):
        return BUY_TYPE_ID in type_ids
    return type_ids == BUY_TYPE_ID


def get_structure(cf):
    raw = cf.get(STRUCTURE_FIELD)
    if isinstance(raw, list):
        raw = raw[0] if raw else None
    if raw is None:
        return "Other"
    try:
        v = int(float(str(raw)))
        if v == FUND_STRUCTURE_ID:
            return "Fund"
        if v == DIRECT_STRUCTURE_ID:
            return "Direct"
        if v == FORWARD_STRUCTURE_ID:
            return "Forward"
        return "Other"
    except Exception:
        return "Other"


def deal_is_forward_only(deal):
    """True if the deal's structure is Forward and nothing else."""
    raw = (deal.get("custom_fields", {}) or {}).get(STRUCTURE_FIELD)
    ids = raw if isinstance(raw, list) else ([raw] if raw else [])
    parsed = []
    for v in ids:
        try:
            parsed.append(int(float(str(v))))
        except Exception:
            continue
    return parsed == [FORWARD_STRUCTURE_ID]


def person_accepts_forwards(cf):
    """True if the person's Accepts field includes Forwards."""
    raw = cf.get(ACCEPTS_FIELD)
    ids = raw if isinstance(raw, list) else ([raw] if raw else [])
    for v in ids:
        try:
            if int(float(str(v))) == ACCEPTS_FORWARDS_ID:
                return True
        except Exception:
            continue
    return False


def parse_pipeline_ts(s):
    if not s:
        return None
    for fmt in ("%Y/%m/%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S %z",
                "%Y/%m/%d %H:%M:%S",    "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def rel_time(iso_str):
    """Human-relative time from an ISO timestamp: 'just now', '4 hrs ago', '3 days ago'."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except Exception:
        return iso_str[:16]
    secs = (datetime.now(timezone.utc) - dt).total_seconds()
    if secs < 0:
        secs = 0
    if secs < 60:
        return "just now"
    mins = int(secs // 60)
    if mins < 60:
        return f"{mins} min ago"
    hrs = int(secs // 3600)
    if hrs < 24:
        return f"{hrs} hr{'s' if hrs != 1 else ''} ago"
    days = int(secs // 86400)
    if days < 31:
        return f"{days} day{'s' if days != 1 else ''} ago"
    months = days // 30
    return f"{months} month{'s' if months != 1 else ''} ago"


def parse_size(cf, field):
    """Returns deal size field as float, or None if missing/zero."""
    v = cf.get(field)
    if isinstance(v, list):
        v = v[0] if v else None
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return f if f > 0 else None
    except Exception:
        return None


def fmt_size(v):
    if v is None:
        return None
    if v >= 1_000_000:
        n = v / 1_000_000
        return f"${n:.0f}M" if n == int(n) else f"${n:.1f}M"
    if v >= 1_000:
        n = v / 1_000
        return f"${n:.0f}K" if n == int(n) else f"${n:.1f}K"
    return f"${int(v)}"


def get_layer(cf):
    raw = cf.get(LAYERS_FIELD)
    if isinstance(raw, list):
        raw = raw[0] if raw else None
    if raw is None:
        return None
    try:
        return LAYERS_MAP.get(int(float(str(raw))))
    except Exception:
        return None


def deal_line(deal):
    cf   = deal.get("custom_fields", {})
    side = "Seller" if is_sell_deal(cf) else "Buyer"

    deal_min = parse_size(cf, MIN_SIZE_FIELD)
    deal_max = parse_size(cf, MAX_SIZE_FIELD)
    if deal_min is not None and deal_max is not None:
        if deal_min == deal_max:
            size_str = fmt_size(deal_min)
        else:
            size_str = f"{fmt_size(deal_min)}–{fmt_size(deal_max)}"
    elif deal_min is not None:
        size_str = f"Min {fmt_size(deal_min)}"
    elif deal_max is not None:
        size_str = f"Max {fmt_size(deal_max)}"
    else:
        size_str = None

    layer = get_layer(cf)
    if layer:
        structure_label = f"{layer} SPV"
    else:
        structure_label = get_structure(cf)

    gross = parse_size(cf, GROSS_FIELD)
    price_str = f"@ ${gross:,.2f}" if gross is not None else None

    inner = f"{size_str} | {structure_label}" if size_str else structure_label
    if price_str:
        inner = f"{inner} {price_str}"
    return f"  New {side} ({inner}) → {TRADES_URL}/deal/{deal['id']}"


def get_person_ticket_range(cf):
    """
    Returns (min_ticket, max_ticket) for the person.
    min_ticket: lowest lower-bound across all their ticket tiers
    max_ticket: highest upper-bound (None = no limit)
    Returns (None, None) if no ticket size set.
    """
    entry_ids = cf.get(TICKET_SIZE_FIELD) or []
    if not entry_ids:
        return None, None
    mins = []
    maxs = []
    for eid in entry_ids:
        tier = TICKET_SIZE_MAP.get(eid)
        if tier:
            mins.append(tier[0])
            maxs.append(tier[1])  # may be None
    if not mins:
        return None, None
    person_min = min(mins)
    # If any tier has no upper limit, person_max = None (unlimited)
    person_max = None if any(m is None for m in maxs) else max(maxs)
    return person_min, person_max


def deal_in_range(deal, person_min, person_max):
    if person_min is None and person_max is None:
        return True
    cf       = deal.get("custom_fields", {})
    deal_min = parse_size(cf, MIN_SIZE_FIELD)
    deal_max = parse_size(cf, MAX_SIZE_FIELD)
    if deal_min is not None and person_max is not None:
        if deal_min > person_max:
            return False
    if deal_max is not None and person_min is not None:
        if deal_max < person_min:
            return False
    return True


def load_field_entries(field_id, jwt):
    name_by_entry = {}
    res = call_pipeline_api(f"/admin/person_custom_field_labels/{field_id}.json", jwt)
    if res["status"] == 200:
        for e in res["data"].get("custom_field_label_dropdown_entries", []):
            name_by_entry[e["id"]] = e["name"]
        logger.info(f"Field {field_id}: loaded {len(name_by_entry)} entries")
    else:
        logger.error(f"Failed to load field {field_id}: {res['status']}")
    return name_by_entry


# ── Admin table (manual alerts) ───────────────────────────────────────────────

ADMIN_KEY      = "JK8h5Pq2L9aZ7rT3mN6bX"
ALERTS_LOG_KEY = "notifier-alerts.json"


def load_alert_history():
    """{deal_id_str: {"last_alerted": iso_ts, "recipients_count": int}} — never raises."""
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=SNAPSHOT_BUCKET, Key=ALERTS_LOG_KEY)
        return json.loads(obj["Body"].read()) or {}
    except Exception:
        return {}


def save_alert_history(history):
    s3 = boto3.client("s3")
    s3.put_object(Bucket=SNAPSHOT_BUCKET, Key=ALERTS_LOG_KEY,
                  Body=json.dumps(history).encode(),
                  ContentType="application/json")


def parse_post_body(event):
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode()
    params = parse_qs(body)
    return {k: v[0] for k, v in params.items()}


def fetch_live_deal(deal_id, jwt):
    res = call_pipeline_api(f"/deals/{deal_id}.json", jwt)
    if res["status"] != 200:
        return None
    data = res["data"]
    if isinstance(data, dict):
        if isinstance(data.get("deal"), dict):
            return data["deal"]
        if "id" in data:
            return data
    return None


def simple_page(title, inner):
    page = ("<!DOCTYPE html><html><head><meta charset='utf-8'><title>" + title +
            "</title><style>" + ADMIN_CSS + "</style></head><body><h1>" + title +
            "</h1>" + inner + "</body></html>")
    return {"statusCode": 200,
            "headers": {"Content-Type": "text/html; charset=utf-8"},
            "body": page}


def recipients_table_html(recipients):
    rows = []
    for r in recipients:
        rows.append(
            "<tr><td>" + h_escape(r.get("name") or "") + "</td>"
            "<td>" + h_escape(r.get("company") or "") + "</td>"
            "<td>" + h_escape(r.get("email") or "") + "</td></tr>"
        )
    return ("<table><tr><th>Name</th><th>Company</th><th>Email</th></tr>"
            + "".join(rows) + "</table>")


def render_recipient_report(company, deal_id, recipients, was_dry):
    mode = ""
    if was_dry:
        mode = ("<p><b>DRY RUN</b> — all emails were routed to " + CHAD_EMAIL +
                ". These are the people who WOULD have received it:</p>")
    inner = (
        f"<p>Alert sent for <b>{h_escape(company)}</b> (deal {deal_id}) — "
        f"{len(recipients)} recipient(s).</p>"
        + mode + recipients_table_html(recipients)
        + f"<p><a href='?key={ADMIN_KEY}'>&larr; Back to deals table</a></p>"
    )
    return simple_page("Alert report", inner)


def render_recipients_view(event):
    qs = event.get("queryStringParameters") or {}
    if qs.get("key") != ADMIN_KEY:
        return {"statusCode": 403,
                "headers": {"Content-Type": "text/plain"},
                "body": "Forbidden"}
    deal_id = qs.get("deal_id", "")
    hist = load_alert_history().get(str(deal_id))
    if not hist:
        return simple_page("No record",
            f"<p>No alert history for deal {h_escape(deal_id)}.</p>"
            f"<p><a href='?key={ADMIN_KEY}'>&larr; Back</a></p>")
    mode = " (DRY RUN — routed to Chad)" if hist.get("dry_run") else ""
    inner = (
        f"<p><b>{h_escape(hist.get('company') or '')}</b> — deal {h_escape(deal_id)}<br>"
        f"Alerted {h_escape(rel_time(hist.get('last_alerted') or ''))}{mode} — "
        f"{hist.get('recipients_count')} recipient(s).</p>"
        + recipients_table_html(hist.get("recipients") or [])
        + f"<p><a href='?key={ADMIN_KEY}'>&larr; Back to deals table</a></p>"
    )
    return simple_page("Alert recipients", inner)


def handle_alert_post(event):
    form = parse_post_body(event)
    if form.get("key") != ADMIN_KEY:
        return {"statusCode": 403,
                "headers": {"Content-Type": "text/plain"},
                "body": "Forbidden"}
    try:
        deal_id = int(form.get("deal_id", ""))
    except ValueError:
        return simple_page("Error", "<p>Missing or invalid deal_id.</p>")

    jwt  = get_jwt()
    deal = fetch_live_deal(deal_id, jwt)
    if deal is None:
        return simple_page("Error",
            f"<p>Could not fetch deal {deal_id} live from Pipeline. Nothing sent.</p>")

    stage_id = (deal.get("deal_stage") or {}).get("id")
    if stage_id not in ACTIVE_STAGES or deal.get("is_archived"):
        return simple_page("Not sent",
            f"<p>Deal {deal_id} is no longer in an active stage. Nothing sent.</p>")

    company = ((deal.get("company") or {}).get("name") or "").strip()
    people_data = load_snapshot("people.json")
    all_people  = people_data.get("people", [])
    buying_name_by_entry  = load_field_entries(3322093, jwt)
    selling_name_by_entry = load_field_entries(3759156, jwt)
    matches = find_matches_for_deal(deal, all_people,
                                    buying_name_by_entry, selling_name_by_entry)
    if not matches:
        return simple_page("Not sent",
            f"<p>No counterparty matches for {h_escape(company)} "
            f"(deal {deal_id}). Nothing sent.</p>")

    # Idempotency lock: one alert per deal per day; blocks double-clicks and bots.
    s3 = boto3.client("s3")
    lock_suffix = "-dry" if DRY_RUN else ""
    lock_key = f"alert-locks/{deal_id}/{date.today().isoformat()}{lock_suffix}.lock"
    try:
        s3.put_object(Bucket=SNAPSHOT_BUCKET, Key=lock_key,
                      Body=b"1", IfNoneMatch="*")
    except Exception as e:
        if "PreconditionFailed" in str(e) or "412" in str(e):
            return simple_page("Already sent",
                f"<p>An alert for deal {deal_id} was already sent today. "
                f"Nothing sent.</p>"
                f"<p><a href='?key={ADMIN_KEY}'>&larr; Back</a></p>")
        return simple_page("Error",
            f"<p>Lock error: {h_escape(str(e))}. Nothing sent.</p>")

    today   = date.today().strftime("%B %d, %Y")
    subject = f"Gracia Group — New activity on {company} — {today}"
    recipients = []
    for person in matches:
        email      = (person.get("email") or "").strip()
        first_name = (person.get("first_name") or "").strip()
        full_name  = f"{first_name} {(person.get('last_name') or '')}".strip()
        greeting   = first_name or full_name or "there"
        form_url   = f"{INTEREST_FORM_URL.rstrip('/')}/?person_id={person['id']}&token={make_token(person['id'])}"
        lines = [
            f"Hello {greeting},",
            "",
            "New activity on a trade you are following:",
            "",
            company,
            deal_summary(deal),
            f"{TRADES_URL}/deal/{deal_id}",
            "",
            "─" * 22,
            "",
            "Not an offer to buy or sell securities.",
            "",
            "UNSUBSCRIBE OR UPDATE YOUR BUY/SELL PREFERENCES:",
            form_url,
            "",
            DISCLOSURE,
        ]
        body = "\n".join(lines)
        if DRY_RUN:
            header = f"[DRY RUN] Real recipient: {email} ({full_name})\n\n"
            send_email(CHAD_EMAIL, f"[DRY RUN] {subject}", header + body)
        else:
            send_email(email, subject, body)
        time.sleep(0.5)
        recipients.append({
            "name": full_name or "(no name)",
            "company": (person.get("company_name") or "").strip(),
            "email": email,
        })
        logger.info(f"{'[DRY] ' if DRY_RUN else ''}Alert deal {deal_id} -> {email}")

    history = load_alert_history()
    history[str(deal_id)] = {
        "last_alerted": datetime.now(timezone.utc).isoformat(),
        "recipients_count": len(recipients),
        "dry_run": DRY_RUN,
        "company": company,
        "recipients": recipients,
    }
    save_alert_history(history)

    return render_recipient_report(company, deal_id, recipients, DRY_RUN)


ADMIN_JS = """
function alertCounterparties(dealId, company, matches) {
  var msg = "Send alert for " + company + " to " + matches + " counterpart" +
            (matches === 1 ? "y" : "ies") + "?";
  if (!confirm(msg)) { return; }
  var f = document.createElement("form");
  f.method = "POST";
  f.action = window.location.pathname;
  var k = document.createElement("input");
  k.type = "hidden"; k.name = "key"; k.value = ADMIN_KEY_JS;
  f.appendChild(k);
  var d = document.createElement("input");
  d.type = "hidden"; d.name = "deal_id"; d.value = dealId;
  f.appendChild(d);
  document.body.appendChild(f);
  f.submit();
}
"""


def find_matches_for_deal(deal, all_people, buying_name_by_entry, selling_name_by_entry):
    """People who would be alerted for this deal, using the same rules as the digest:
    whitelist tag, broadcast not No/Hold, has email, interest matches company,
    ticket range fits, forward-only deals require Accepts Forwards."""
    cf_deal = deal.get("custom_fields", {})
    company = ((deal.get("company") or {}).get("name") or "").strip().lower()
    if not company:
        return []
    if is_sell_deal(cf_deal):
        interest_field, name_map = BUYING_FIELD, buying_name_by_entry
    elif is_buy_deal(cf_deal):
        interest_field, name_map = SELLING_FIELD, selling_name_by_entry
    else:
        return []
    fwd_only = deal_is_forward_only(deal)
    matches = []
    for person in all_people:
        email = (person.get("email") or "").strip()
        if not email:
            continue
        if WHITELIST_TAG_ID not in (person.get("predefined_contacts_tag_ids") or []):
            continue
        cf = person.get("custom_fields", {})
        braw = cf.get(BROADCAST_FIELD)
        bids = braw if isinstance(braw, list) else ([braw] if braw else [])
        if BROADCAST_NO_ID in bids or BROADCAST_HOLD_ID in bids:
            continue
        names = set()
        for eid in (cf.get(interest_field) or []):
            nm = name_map.get(eid, "")
            if nm:
                names.add(nm.strip().lower())
        if company not in names:
            continue
        pmin, pmax = get_person_ticket_range(cf)
        if not deal_in_range(deal, pmin, pmax):
            continue
        if fwd_only and not person_accepts_forwards(cf):
            continue
        matches.append(person)
    return matches


def deal_summary(deal):
    """Compact 'Seller | $1M–$5M | Direct @ $85.00' style string."""
    cf   = deal.get("custom_fields", {})
    side = "Seller" if is_sell_deal(cf) else ("Buyer" if is_buy_deal(cf) else "?")
    dmin = parse_size(cf, MIN_SIZE_FIELD)
    dmax = parse_size(cf, MAX_SIZE_FIELD)
    if dmin is not None and dmax is not None:
        size_str = fmt_size(dmin) if dmin == dmax else f"{fmt_size(dmin)}–{fmt_size(dmax)}"
    elif dmin is not None:
        size_str = f"Min {fmt_size(dmin)}"
    elif dmax is not None:
        size_str = f"Max {fmt_size(dmax)}"
    else:
        size_str = ""
    layer = get_layer(cf)
    structure = f"SPV {layer[0]}L" if layer else get_structure(cf)
    sf = cf.get(SELLER_FEE_FIELD)
    mf = cf.get(MGMT_FEE_FIELD)
    cy = cf.get(CARRY_FIELD)
    if any(v not in (None, "", []) for v in (sf, mf, cy)):
        fees = f" {fmt_fee(sf)}/{fmt_fee(mf)}/{fmt_fee(cy)}"
    else:
        fees = ""
    gross = parse_size(cf, GROSS_FIELD)
    price = f" @ ${gross:,.2f}" if gross is not None else ""
    parts = [side]
    if size_str:
        parts.append(size_str)
    parts.append(f"{structure}{fees}{price}")
    return " | ".join(parts)


ADMIN_CSS = """
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       margin: 24px; background: #f7f8fa; color: #1a1a2e; }
h1 { font-size: 20px; }
table { border-collapse: collapse; width: 100%; background: #fff;
        box-shadow: 0 1px 3px rgba(0,0,0,.08); }
th, td { padding: 8px 12px; border-bottom: 1px solid #e5e7eb;
         text-align: left; font-size: 13px; }
th { background: #111827; color: #fff; position: sticky; top: 0; }
tr:hover td { background: #f0f4ff; }
.num { text-align: right; }
.muted { color: #9ca3af; }
button.alert-btn { background: #2563eb; color: #fff; border: none;
                   padding: 5px 10px; border-radius: 5px; cursor: pointer; }
button.alert-btn:hover { background: #1d4ed8; }
a { color: #2563eb; text-decoration: none; }
"""


def render_admin_table(event):
    qs  = event.get("queryStringParameters") or {}
    if qs.get("key") != ADMIN_KEY:
        return {"statusCode": 403,
                "headers": {"Content-Type": "text/plain"},
                "body": "Forbidden"}

    jwt         = get_jwt()
    deals_data  = load_snapshot("deals.json")
    people_data = load_snapshot("people.json")
    all_deals   = deals_data.get("deals", [])
    all_people  = people_data.get("people", [])
    buying_name_by_entry  = load_field_entries(3322093, jwt)
    selling_name_by_entry = load_field_entries(3759156, jwt)
    history     = load_alert_history()

    rows = []
    for deal in all_deals:
        stage_id = (deal.get("deal_stage") or {}).get("id")
        if stage_id not in ACTIVE_STAGES or deal.get("is_archived"):
            continue
        cf = deal.get("custom_fields", {})
        nexus_ids = cf.get(NEXUS_FIELD) or []
        if not isinstance(nexus_ids, list):
            nexus_ids = [nexus_ids]
        nexus_label = ", ".join(NEXUS_NAMES.get(n, "?") for n in nexus_ids) or "—"
        company = ((deal.get("company") or {}).get("name") or "").strip()
        if not company:
            continue
        updated = parse_pipeline_ts(deal.get("updated_at"))
        matches = find_matches_for_deal(deal, all_people,
                                        buying_name_by_entry, selling_name_by_entry)
        hist = history.get(str(deal["id"])) or {}
        rows.append({
            "id": deal["id"],
            "company": company,
            "summary": deal_summary(deal),
            "stage": "Firm" if stage_id == FIRM_STAGE_ID else "Inquiry",
            "nexus": nexus_label,
            "updated": updated,
            "updated_str": (deal.get("updated_at") or "")[:16],
            "match_count": len(matches),
            "last_alerted": rel_time(hist.get("last_alerted") or ""),
            "sent_count": hist.get("recipients_count"),
            "was_dry": bool(hist.get("dry_run")),
        })

    rows.sort(key=lambda r: r["updated"] or datetime.min.replace(tzinfo=timezone.utc),
              reverse=True)
    rows = rows[:150]

    body_rows = []
    for r in rows:
        comp = h_escape(r["company"])
        summ = h_escape(r["summary"])
        if r["sent_count"] is None:
            sent_cell = ""
        else:
            dry_tag = " (dry)" if r["was_dry"] else ""
            sent_cell = (f"<a href='?key={ADMIN_KEY}&view=recipients&deal_id={r['id']}'>"
                         f"{r['sent_count']}{dry_tag}</a>")
        last = r["last_alerted"] or '<span class="muted">never</span>'
        onclick_company = h_escape(json.dumps(r["company"]), quote=True)
        body_rows.append(
            f"<tr>"
            f"<td><a href='{TRADES_URL}/deal/{r['id']}' target='_blank'>{comp}</a></td>"
            f"<td>{r['stage']}</td>"
            f"<td>{r['nexus']}</td>"
            f"<td>{summ}</td>"
            f"<td>{r['updated_str']}</td>"
            f"<td class='num'>{r['match_count']}</td>"
            f"<td>{last}</td>"
            f"<td class='num'>{sent_cell}</td>"
            f"<td><button class='alert-btn' "
            f"onclick='alertCounterparties({r['id']}, {onclick_company}, {r['match_count']})'>"
            f"Alert counterparties</button></td>"
            f"</tr>"
        )

    html = (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        "<title>Deal Notifier — Admin</title>"
        "<style>" + ADMIN_CSS + "</style>"
        "<script>var ADMIN_KEY_JS=" + json.dumps(ADMIN_KEY) + ";" + ADMIN_JS + "</script>"
        "</head><body>"
        "<h1>Deal Notifier — Manual Alerts</h1>"
        f"<p class='muted'>{len(rows)} active deals (all nexus types), newest first. "
        "Match counts use the same rules as the digest "
        "(whitelist, broadcast, interests, ticket size, forwards).</p>"
        "<table><tr><th>Company</th><th>Stage</th><th>Nexus</th><th>Deal</th><th>Updated</th>"
        "<th>Matches</th><th>Last alerted</th><th>Sent</th><th></th></tr>"
        + "".join(body_rows) +
        "</table></body></html>"
    )
    return {"statusCode": 200,
            "headers": {"Content-Type": "text/html; charset=utf-8"},
            "body": html}


# ── Main handler ──────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    method = ((event.get("requestContext") or {}).get("http") or {}).get("method")
    if method == "POST":
        return handle_alert_post(event)
    if method:
        qs = event.get("queryStringParameters") or {}
        if qs.get("view") == "recipients":
            return render_recipients_view(event)
        return render_admin_table(event)
    return run_digest(event, context)


def run_digest(event, context):
    today  = date.today().strftime("%B %d, %Y")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    logger.info(f"Recency filter: lookback={LOOKBACK_HOURS}h, cutoff={cutoff.isoformat()}")
    jwt    = get_jwt()

    # Load snapshots
    logger.info("Loading snapshots")
    deals_data  = load_snapshot("deals.json")
    people_data = load_snapshot("people.json")
    all_deals   = deals_data.get("deals", [])
    all_people  = people_data.get("people", [])
    logger.info(f"Loaded {len(all_deals)} deals, {len(all_people)} people")

    # Load security entry ID → name mappings
    logger.info("Loading interest field mappings")
    buying_name_by_entry  = load_field_entries(3322093, jwt)
    selling_name_by_entry = load_field_entries(3759156, jwt)
    live_auctions = load_live_auctions()

    # Build deal index by company name
    sell_deals_by_name = {}
    buy_deals_by_name  = {}
    for deal in all_deals:
        stage_id = (deal.get("deal_stage") or {}).get("id")
        if stage_id not in ACTIVE_STAGES or deal.get("is_archived"):
            continue
        company_name = (deal.get("company") or {}).get("name", "").strip()
        if not company_name:
            continue
        key = company_name.lower()
        deal_cf = deal.get("custom_fields", {})

        updated = parse_pipeline_ts(deal.get("updated_at"))
        if updated is None or updated < cutoff:
            continue

        nexus_ids = deal_cf.get(NEXUS_FIELD) or []
        if not isinstance(nexus_ids, list):
            nexus_ids = [nexus_ids]
        if NEXUS_DIRECT_ID not in nexus_ids:
            continue

        # ── TEMPORARY: noise filter for high-volume companies ─────────────────
        # Anthropic, Anduril, and SpaceX have a backlog of Inquiry-stage and
        # priceless deals that clutter the daily digest with inactionable
        # lines. For these companies only, require Firm + side-appropriate
        # price (gross for buys, net for sells). Remove this block — and the
        # NOISY_COMPANIES constant — once the backlog clears.
        # ──────────────────────────────────────────────────────────────────────
        if key in NOISY_COMPANIES:
            if stage_id != FIRM_STAGE_ID:
                continue
            if is_sell_deal(deal_cf):
                if parse_size(deal_cf, NET_FIELD) is None:
                    continue
            elif is_buy_deal(deal_cf):
                if parse_size(deal_cf, GROSS_FIELD) is None:
                    continue

        if is_sell_deal(deal_cf):
            sell_deals_by_name.setdefault(key, []).append(deal)
        elif is_buy_deal(deal_cf):
            buy_deals_by_name.setdefault(key, []).append(deal)
        # deals with no type set are skipped

    logger.info(f"Deal index: {len(sell_deals_by_name)} sell, {len(buy_deals_by_name)} buy companies")
    logger.info(f"Sell companies: {sorted(sell_deals_by_name.keys())}")
    logger.info(f"Buy companies:  {sorted(buy_deals_by_name.keys())}")

    # Process each person
    emails_sent = 0
    no_match    = 0
    no_email    = 0

    for person in all_people:
        if emails_sent >= MAX_EMAILS:
            logger.info(f"MAX_EMAILS ({MAX_EMAILS}) reached — stopping")
            break

        email      = (person.get("email") or "").strip()
        first_name = (person.get("first_name") or "").strip()
        full_name  = f"{first_name} {(person.get('last_name') or '')}".strip()
        cf         = person.get("custom_fields", {})

        if not email:
            no_email += 1
            continue

        if WHITELIST_TAG_ID not in (person.get("predefined_contacts_tag_ids") or []):
            continue

        broadcast_raw = cf.get(BROADCAST_FIELD)
        if isinstance(broadcast_raw, list):
            broadcast_ids = broadcast_raw
        else:
            broadcast_ids = [broadcast_raw] if broadcast_raw else []
        if BROADCAST_NO_ID in broadcast_ids or BROADCAST_HOLD_ID in broadcast_ids:
            continue

        person_min, person_max = get_person_ticket_range(cf)

        # Find sell opportunities (person wants to buy)
        sell_opps = {}
        for entry_id in (cf.get(BUYING_FIELD) or []):
            sec_name = buying_name_by_entry.get(entry_id, "")
            if not sec_name:
                continue
            matches = [
                d for d in sell_deals_by_name.get(sec_name.lower(), [])
                if deal_in_range(d, person_min, person_max)
                and not (deal_is_forward_only(d) and not person_accepts_forwards(cf))
            ]
            if matches:
                sell_opps[sec_name] = matches

        # Find buy opportunities (person wants to sell)
        buy_opps = {}
        for entry_id in (cf.get(SELLING_FIELD) or []):
            sec_name = selling_name_by_entry.get(entry_id, "")
            if not sec_name:
                continue
            matches = [
                d for d in buy_deals_by_name.get(sec_name.lower(), [])
                if deal_in_range(d, person_min, person_max)
                and not (deal_is_forward_only(d) and not person_accepts_forwards(cf))
            ]
            if matches:
                buy_opps[sec_name] = matches

        if not sell_opps and not buy_opps:
            no_match += 1
            continue

        form_url = f"{INTEREST_FORM_URL.rstrip('/')}/?person_id={person['id']}&token={make_token(person['id'])}"

        greeting = first_name or full_name or "there"
        lines = [
            f"Hello {greeting},",
            "",
            "New activity on trades you are following:",
        ]

        for sec_name in sorted(sell_opps):
            lines.append("")
            lines.append(sec_name)
            for d in sell_opps[sec_name]:
                lines.append(deal_line(d))

        for sec_name in sorted(buy_opps):
            lines.append("")
            lines.append(sec_name)
            for d in buy_opps[sec_name]:
                lines.append(deal_line(d))

        my_buy_names = set()
        for entry_id in (cf.get(BUYING_FIELD) or []):
            nm = buying_name_by_entry.get(entry_id, "")
            if nm:
                my_buy_names.add(_auction_base_name(nm))
        auction_lines = []
        for a in live_auctions:
            if _auction_base_name(a["company"]) not in my_buy_names:
                continue
            link = (f"{PORTFOLIO_URL}/?client={person['id']}"
                    f"&token={make_portfolio_token(person['id'])}"
                    f"&view=auction&id={a['id']}")
            bits = []
            if a["top"] is not None:
                bits.append(f"top bid ${a['top']:,.2f}")
            if a["close_date"]:
                bits.append(f"bids close {a['close_date']}")
            detail = " | ".join(bits) if bits else "no bids yet"
            auction_lines.append("")
            auction_lines.append(f"{a['company']} — LIVE AUCTION ({detail})")
            auction_lines.append(f"  Place or raise a bid → {link}")
        if auction_lines:
            lines.append("")
            lines.append("─" * 22)
            lines.append("")
            lines.append("LIVE AUCTIONS you can bid on now:")
            lines += auction_lines

        lines += [
            "",
            "─" * 22,
            "",
            "Not an offer to buy or sell securities.",
            "",
            "UNSUBSCRIBE OR UPDATE YOUR BUY/SELL PREFERENCES:",
            form_url,
            "",
            DISCLOSURE,
        ]

        body    = "\n".join(lines)
        subject = f"Gracia Group — New activity on trades you are following — {today}"

        if DRY_RUN:
            header = (
                f"[DRY RUN] Real recipient: {email} ({full_name})\n"
                f"Ticket range: ${person_min:,} – {'unlimited' if person_max is None else f'${person_max:,}'}\n\n"
                if person_min is not None else
                f"[DRY RUN] Real recipient: {email} ({full_name})\nTicket range: not set\n\n"
            )
            send_email(CHAD_EMAIL, f"[DRY RUN] {subject}", header + body)
            time.sleep(0.5)
        else:
            send_email(email, subject, body)
            time.sleep(0.5)

        emails_sent += 1
        logger.info(f"{'[DRY] ' if DRY_RUN else ''}Sent → {email} ({full_name})")

    logger.info(f"Done — sent: {emails_sent}, no_match: {no_match}, no_email: {no_email}")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "emails_sent": emails_sent,
            "no_match":    no_match,
            "no_email":    no_email,
            "dry_run":     DRY_RUN,
        })
    }
