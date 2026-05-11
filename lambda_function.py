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
INTEREST_FORM_URL   = "https://mrp5bv4iia7jxjfrvn67tpycsu0jqvny.lambda-url.us-east-1.on.aws/"

DRY_RUN             = os.environ.get("DRY_RUN", "true").lower() == "true"
MAX_EMAILS          = int(os.environ.get("MAX_EMAILS", "10"))
LOOKBACK_HOURS      = int(os.environ["LOOKBACK_HOURS"])
HMAC_SECRET         = os.environ["HMAC_SECRET"]

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
NEXUS_FIELD         = "custom_label_3751449"
NEXUS_DIRECT_ID     = 6460632
LAYERS_FIELD        = "custom_label_3938743"
LAYERS_MAP          = {7000228: "1-Layer", 7000229: "2-Layer", 7000230: "3-Layer"}

# Active deal stages
FIRM_STAGE_ID       = 111800
INQUIRY_STAGE_ID    = 2109142
ACTIVE_STAGES       = {FIRM_STAGE_ID, INQUIRY_STAGE_ID}

# TEMPORARY — see indexing loop. Remove together with the noise filter
# block once the backlog on these names clears.
NOISY_COMPANIES     = {"anthropic", "anduril", "spacex"}

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
        return "Other"
    except Exception:
        return "Other"


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


# ── Main handler ──────────────────────────────────────────────────────────────

def lambda_handler(event, context):
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

        lines += [
            "",
            "─" * 22,
            "",
            "Questions about anything in this digest? Reply to this email or "
            "write directly to cgracia@rainmakersecurities.com.",
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
