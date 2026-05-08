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
import urllib.request
import urllib.error
import os
import boto3
from datetime import date

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config ────────────────────────────────────────────────────────────────────
SNAPSHOT_BUCKET     = "full-pipeline-cache"
JWT_BUCKET          = "pipeline-token"
JWT_KEY             = "pipeline-jwt.json"
SES_SENDER          = "agent@agent.graciagroup.com"
CHAD_EMAIL          = "cgracia@rainmakersecurities.com"
TRADES_URL          = "https://trades.graciagroup.com"

DRY_RUN             = os.environ.get("DRY_RUN", "true").lower() == "true"
MAX_EMAILS          = int(os.environ.get("MAX_EMAILS", "10"))

# Person fields
BUYING_FIELD        = "custom_label_3322093"
SELLING_FIELD       = "custom_label_3759156"
TICKET_SIZE_FIELD   = "custom_label_3052210"

# Ticket size entry ID → (min, max) in dollars
# min = lower bound of range, max = upper bound (None = no upper limit)
TICKET_SIZE_MAP = {
    6870210: (100_000,       None),          # > 100K
    6631962: (0,             250_000),       # $250K
    5014552: (0,             1_000_000),     # - $1M
    5014555: (1_000_000,     5_000_000),     # $1M - $5M
    5014558: (5_000_000,     10_000_000),    # $5M - $10M
    5014561: (10_000_000,    25_000_000),    # $10M - $25M
    5014564: (25_000_000,    50_000_000),    # $25M - $50M
    5014567: (50_000_000,    100_000_000),   # $50M - $100M
    5014570: (100_000_000,   None),          # $100M+
}

# Deal custom fields
DEAL_TYPE_FIELD     = "custom_label_1958"
SELL_TYPE_ID        = 5011675
MIN_SIZE_FIELD      = "custom_label_3065488"
MAX_SIZE_FIELD      = "custom_label_3064645"
STRUCTURE_FIELD     = "custom_label_3064360"
FUND_STRUCTURE_ID   = 5077906
DIRECT_STRUCTURE_ID = 6250090

# Active deal stages
FIRM_STAGE_ID       = 111800
INQUIRY_STAGE_ID    = 2109142
ACTIVE_STAGES       = {FIRM_STAGE_ID, INQUIRY_STAGE_ID}


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
        Source=SES_SENDER,
        Destination={"ToAddresses": [to_address]},
        Message={
            "Subject": {"Data": subject},
            "Body":    {"Text": {"Data": body}}
        }
    )


def is_sell_deal(cf):
    type_ids = cf.get(DEAL_TYPE_FIELD, [])
    if isinstance(type_ids, list):
        return SELL_TYPE_ID in type_ids
    return type_ids == SELL_TYPE_ID


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


def deal_line(deal):
    cf        = deal.get("custom_fields", {})
    side      = "Seller" if is_sell_deal(cf) else "Buyer"
    structure = get_structure(cf)
    return f"  New {side} ({structure}) → {TRADES_URL}/deal/{deal['id']}"


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
    today = date.today().strftime("%B %d, %Y")
    jwt   = get_jwt()

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
        if is_sell_deal(deal.get("custom_fields", {})):
            sell_deals_by_name.setdefault(key, []).append(deal)
        else:
            buy_deals_by_name.setdefault(key, []).append(deal)

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
            "To update your buy/sell interests, reply to this email.",
            "To unsubscribe reply with \"unsubscribe\".",
            "Not an offer to buy or sell securities.",
        ]

        body    = "\n".join(lines)
        subject = f"New activity on trades you are following — {today}"

        if DRY_RUN:
            header = (
                f"[DRY RUN] Real recipient: {email} ({full_name})\n"
                f"Ticket range: ${person_min:,} – {'unlimited' if person_max is None else f'${person_max:,}'}\n\n"
                if person_min is not None else
                f"[DRY RUN] Real recipient: {email} ({full_name})\nTicket range: not set\n\n"
            )
            send_email(CHAD_EMAIL, f"[DRY RUN] {subject}", header + body)
        else:
            send_email(email, subject, body)

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
