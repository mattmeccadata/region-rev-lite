# If needed (uncomment and run once):
# !pip install requests pandas rapidfuzz python-dateutil

import os, time, json, math
import requests
import pandas as pd
from rapidfuzz import fuzz, process
from dateutil import tz

MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN") or "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjE4MjM0MTAwOSwiYWFpIjoxMSwidWlkIjoyNjg1NTM4NywiaWFkIjoiMjAyMi0wOS0yMlQxMTo0OToxNy4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MzAzNTU1NSwicmduIjoidXNlMSJ9.I3wDWG-yICZO3WOmqh-0QxEwXp5hUXzwUi9g5hHREW0"
API_URL = "https://api.monday.com/v2"
API_VERSION = "2025-04"  # supports items_page + typed column_values; good forward-compat

session = requests.Session()
session.headers.update({
    "Authorization": MONDAY_API_TOKEN,
    "Content-Type": "application/json",
    "API-Version": API_VERSION
})

def gql(query: str, variables: dict=None, max_retries: int=5):
    """Minimal helper with gentle retry for 429s."""
    for attempt in range(max_retries):
        resp = session.post(API_URL, json={"query": query, "variables": variables or {}})
        if resp.status_code == 429:
            time.sleep(1.5 * (attempt+1))
            continue
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(data["errors"])
        return data["data"]
    raise RuntimeError("Rate limited repeatedly.")

# We’ll use typed column_value fragments so you can pull normalized fields per column type.
COLUMN_FRAGMENT = """
fragment Cols on ColumnValue {
  id
  text
  type
  ... on NumbersValue { number }
  ... on StatusValue { index text }
  ... on DropdownValue { text }   # <- removed 'ids'
  ... on EmailValue { email text }
  ... on PhoneValue { phone country_short_name text }
  ... on TextValue { text }
  ... on BoardRelationValue { linked_item_ids linked_items { id } }
}
"""

def get_board_columns(board_id: int):
    """Fetch board columns metadata (id, title, type)."""
    q = """
    query($board_id:[ID!]) {
      boards(ids:$board_id) {
        columns { id title type }
      }
    }
    """
    return gql(q, {"board_id":[board_id]})["boards"][0]["columns"]

def items_page_query(board_id: int, limit=500, query_params=None, column_ids=None):
    """
    Read a board page using items_page. Optionally filter with query_params and restrict to column_ids.
    Returns (items, cursor)
    """
    cols_selector = f'column_values(ids: {json.dumps(column_ids)})' if column_ids else "column_values"
    q = f"""
    {COLUMN_FRAGMENT}
    query($board_id:[ID!], $limit:Int!, $query_params: ItemsQuery) {{
      boards(ids:$board_id) {{
        items_page(limit:$limit, query_params:$query_params) {{
          cursor
          items {{
            id
            name
            group {{ id title }}
            {cols_selector} {{ ...Cols }}
          }}
        }}
      }}
    }}
    """
    data = gql(q, {"board_id":[board_id], "limit":limit, "query_params":query_params})
    page = data["boards"][0]["items_page"]
    return page["items"], page["cursor"]

def next_items_page(cursor: str, limit=500, column_ids=None):
    cols_selector = f'column_values(ids: {json.dumps(column_ids)})' if column_ids else "column_values"
    q = f"""
    {COLUMN_FRAGMENT}
    query($cursor:String!, $limit:Int!) {{
      next_items_page(cursor:$cursor, limit:$limit) {{
        cursor
        items {{
          id
          name
          group {{ id title }}
          {cols_selector} {{ ...Cols }}
        }}
      }}
    }}
    """
    data = gql(q, {"cursor": cursor, "limit": limit})
    return data["next_items_page"]["items"], data["next_items_page"]["cursor"]

def fetch_all_items(board_id: int, column_ids=None, query_params=None, per_page=500):
    """Fetch all items using items_page + next_items_page."""
    items, cursor = items_page_query(board_id, limit=per_page, query_params=query_params, column_ids=column_ids)
    all_items = items[:]
    while cursor:
        items, cursor = next_items_page(cursor, limit=per_page, column_ids=column_ids)
        all_items.extend(items)
    return all_items

# Boards
BOARD_PLEDGES = 6704457477         # "Pledges"
BOARD_GIFTS_2025 = 3907842599      # "2025 Gifts"
BOARD_GIFTS_2012_24 = 3782496456   # "2012-24 Gifts"
BOARD_DONORS = 3782435039          # "Donors"

# Pledges board (6704457477) column IDs
P_NAME = "name"  # item name, built-in
P_ID = "id"      # item id
P_COMMITMENT_TYPE = "status__1"
P_TOTAL_COMMITMENT = "numbers__1"
P_REGION = "dropdown2__1"
P_LINKED_GIFTS = "board_relation_mkw4s5jj"
P_EMAIL = "email__1"
P_PHONE = "phone__1"
P_SECOND_PHONE = "dup__of_phone__1"
P_ADDR = "text3__1"
P_CITY = "text1__1"
P_STATE = "dropdown__1"
P_ZIP = "text6__1"

PLEDGE_COLS = [
    P_COMMITMENT_TYPE, P_TOTAL_COMMITMENT, P_REGION, P_LINKED_GIFTS,
    P_EMAIL, P_PHONE, P_SECOND_PHONE, P_ADDR, P_CITY, P_STATE, P_ZIP
]

# 2025 Gifts board (3907842599)
G25_NAME = "name"
G25_ID = "id"
G25_LINKED_DONOR = "connect_boards"
G25_LINKED_SOFT_CREDIT = "connect_boards5"
G25_AMOUNT = "numeric5"
G25_GL = "dropdown"
G25_CLASS = "dropdown66"
G25_PREF = "dropdown8"
G25_SOLIC = "dropdown58"
G25_CHECK = "boolean_mkw54d42"
G25_MAPPED_CLASS = "dropdown_mkvptvde"

GIFTS25_COLS = [G25_LINKED_DONOR, G25_LINKED_SOFT_CREDIT, G25_AMOUNT, G25_GL, G25_CLASS, G25_PREF, G25_SOLIC, G25_CHECK, G25_MAPPED_CLASS]

# 2012-24 Gifts board (3782496456)
GOLD_NAME = "name"
GOLD_ID = "id"
GOLD_LINKED_DONOR = "connect_boards"
GOLD_LINKED_SOFT_CREDIT = "connect_boards5"
GOLD_AMOUNT = "numeric5"
GOLD_GL = "dropdown30"
GOLD_CLASS = "dropdown66"
GOLD_PREF = "dropdown8"
GOLD_SOLIC = "dropdown58"
GOLD_CHECK= "boolean_mkw5kcdd"
GOLD_MAPPED_CLASS = "dropdown_mkwanbpv"

GIFTSOLD_COLS = [GOLD_LINKED_DONOR, GOLD_LINKED_SOFT_CREDIT, GOLD_AMOUNT, GOLD_GL, GOLD_CLASS, GOLD_PREF, GOLD_SOLIC, GOLD_CHECK, GOLD_MAPPED_CLASS]

# Donor board (3782435039)
D_EMAIL = "email"
D_PHONE = "phone"
D_ADDR = "text7"
DONOR_COLS = [D_EMAIL, D_PHONE, D_ADDR]

DATE_COL_OLDGIFTS = "date"

# Build query_params for date > "2024-01-01"
date_filter_params = {
    "rules": [{
        "column_id": DATE_COL_OLDGIFTS,
        "compare_value": ["EXACT", "2024-01-01"],
        "operator": "greater_than"
    }]
}

pledges_raw = fetch_all_items(BOARD_PLEDGES, column_ids=PLEDGE_COLS)
gifts2025_raw = fetch_all_items(BOARD_GIFTS_2025, column_ids=GIFTS25_COLS)
gifts_old_raw = fetch_all_items(BOARD_GIFTS_2012_24, column_ids=GIFTSOLD_COLS, query_params=date_filter_params)

len(pledges_raw), len(gifts2025_raw), len(gifts_old_raw)


def cv_map(item):
    """Return {col_id: column_value_object}"""
    return {cv["id"]: cv for cv in item.get("column_values", [])}

def get_number(cv, default=0.0):
    try:
        return float(cv.get("number")) if cv and cv.get("number") is not None else default
    except:
        return default

def get_text(cv):
    return (cv or {}).get("text")

def get_dropdown_text(cv):
    return (cv or {}).get("text")  # labels concatenated by comma if multiple

def get_status_text(cv):
    return (cv or {}).get("text")

def get_board_relation_ids(cv):
    # BoardRelationValue: use linked_item_ids (preferred per docs)
    ids = (cv or {}).get("linked_item_ids") or []
    # Ensure ints
    return [int(x) for x in ids]

def get_connect_single_id(cv):
    # Some “connect boards” columns may link multiple; you said single for donor/soft-credit
    ids = (cv or {}).get("linked_item_ids") or []
    return int(ids[0]) if ids else None

def get_email(cv):
    return (cv or {}).get("email") or (cv or {}).get("text")

def get_phone(cv):
    return (cv or {}).get("phone") or (cv or {}).get("text")

def get_checkbox(cv):
    return (cv or {}).get("checked") or (cv or {}).get("text")


def pledges_to_df(items):
    rows = []
    for it in items:
        cv = cv_map(it)
        rows.append({
            "pledge_id": int(it["id"]),
            "name": it["name"],
            "group_title": (it.get("group") or {}).get("title"),
            "commitment_type": get_status_text(cv.get(P_COMMITMENT_TYPE)),
            "total_commitment": get_number(cv.get(P_TOTAL_COMMITMENT), 0.0),
            "region": get_dropdown_text(cv.get(P_REGION)),
            "linked_gift_ids": get_board_relation_ids(cv.get(P_LINKED_GIFTS)),
            "email": get_email(cv.get(P_EMAIL)),
            "phone": get_phone(cv.get(P_PHONE)),
            "second_phone": get_phone(cv.get(P_SECOND_PHONE)),
            "addr_lines": get_text(cv.get(P_ADDR)),
            "city": get_text(cv.get(P_CITY)),
            "state": get_dropdown_text(cv.get(P_STATE)),
            "zip": get_text(cv.get(P_ZIP)),
        })
    return pd.DataFrame(rows)

def gifts_to_df(items, is_2025=True):
    rows = []
    for it in items:
        cv = cv_map(it)
        linked_donor = get_connect_single_id(cv.get(G25_LINKED_DONOR if is_2025 else GOLD_LINKED_DONOR))
        linked_soft = get_connect_single_id(cv.get(G25_LINKED_SOFT_CREDIT if is_2025 else GOLD_LINKED_SOFT_CREDIT))
        amount = get_number(cv.get(G25_AMOUNT if is_2025 else GOLD_AMOUNT), 0.0)
        gl = get_dropdown_text(cv.get(G25_GL if is_2025 else GOLD_GL))
        cls = get_dropdown_text(cv.get(G25_CLASS if is_2025 else GOLD_CLASS))
        pref = get_dropdown_text(cv.get(G25_PREF if is_2025 else GOLD_PREF))
        solic = get_dropdown_text(cv.get(G25_SOLIC if is_2025 else GOLD_SOLIC))
        check = get_checkbox(cv.get(G25_CHECK if is_2025 else GOLD_CHECK))
        mapped_class = get_dropdown_text(cv.get(G25_MAPPED_CLASS if is_2025 else GOLD_MAPPED_CLASS))
        rows.append({
            "gift_id": int(it["id"]),
            "name": it["name"],
            "group_title": (it.get("group") or {}).get("title"),  # "2024"/"2025"/etc.
            "linked_donor_id": linked_donor,
            "linked_soft_credit_id": linked_soft,
            "amount": amount,
            "gl_account": gl,
            "class": cls,
            "gift_preference": pref,
            "solicitation": solic,
            "checkbox": check,
            "mapped_class": mapped_class,
            "board": "2025 Gifts" if is_2025 else "2012-24 Gifts",
        })
    return pd.DataFrame(rows)

pledges_df = pledges_to_df(pledges_raw)
gifts25_df = gifts_to_df(gifts2025_raw, is_2025=True)
gifts_old_df = gifts_to_df(gifts_old_raw, is_2025=False)


PAGE_LIMIT = 500  # fixed page size

def _fetch_board_items(board_id):
    q = f"""
    {COLUMN_FRAGMENT}
    query($board_id:[ID!], $cursor:String, $limit:Int!) {{
      boards(ids:$board_id) {{
        items_page(cursor:$cursor, limit:$limit) {{
          cursor
          items {{
            id
            name
            column_values(ids: {json.dumps(DONOR_COLS)}) {{ ...Cols }}
          }}
        }}
      }}
    }}
    """
    items = []
    cursor = None
    while True:
        variables = {"board_id": [board_id], "cursor": cursor, "limit": PAGE_LIMIT}
        res = session.post(API_URL, json={"query": q, "variables": variables}, headers=None)
        data = res.json()
        boards = (data.get("data") or {}).get("boards") or []
        page = boards[0]["items_page"] if boards else {"items": [], "cursor": None}
        items.extend(page.get("items") or [])
        cursor = page.get("cursor")
        if not cursor:
            break
    return items

def fetch_donors_map(candidate_donor_ids):
    # Pull all items from the donors board
    all_items = _fetch_board_items(BOARD_DONORS)

    # Build lookup by integer ID
    by_id = {}
    for it in all_items:
        try:
            by_id[int(it["id"])] = it
        except Exception:
            pass

    # Build output for candidates that exist on the board
    out = {}
    seen = set()
    for x in candidate_donor_ids:
        if x is None:
            continue
        try:
            xid = int(x)
        except Exception:
            continue
        if xid in seen:
            continue
        seen.add(xid)
        it = by_id.get(xid)
        if not it:
            continue
        cv = {c["id"]: c for c in it.get("column_values", [])}
        out[str(it["id"])] = {
            "donor_name": it.get("name"),
            "email": get_email(cv.get(D_EMAIL)),
            "phone": get_phone(cv.get(D_PHONE)),
            "addr_lines": get_text(cv.get(D_ADDR)),
        }
    return out

# Build donor id set (soft-credit preferred; if soft is missing, use donor)
candidate_donor_ids = set()
for df in (gifts25_df, gifts_old_df):
    for _, r in df.iterrows():
        sc = r.get("linked_soft_credit_id")
        dn = r.get("linked_donor_id")
        if pd.notna(sc):
            candidate_donor_ids.add(int(sc))
        elif pd.notna(dn):
            candidate_donor_ids.add(int(dn))

donor_map = fetch_donors_map(candidate_donor_ids)

print("Candidate donor IDs collected:", len(candidate_donor_ids))
print("Valid donor IDs fetched   :", len(donor_map))
print("Sample donors:")
for k, v in list(donor_map.items())[:5]:
    print(k, "→", v)


all_gifts_df = pd.concat([gifts25_df, gifts_old_df], ignore_index=True)
gift_amount_by_id = dict(zip(all_gifts_df["gift_id"], all_gifts_df["amount"]))
gift_gl_by_id = dict(zip(all_gifts_df["gift_id"], all_gifts_df["gl_account"]))
gift_group_by_id = dict(zip(all_gifts_df["gift_id"], all_gifts_df["group_title"]))
gift_mapped_class_by_id = dict(zip(all_gifts_df["gift_id"], all_gifts_df["mapped_class"]))

def summarize_region_gifts(pledges_df):
    rows = []
    for _, p in pledges_df.iterrows():
        region = p["region"]
        g_ids = p.get("linked_gift_ids") or []
        for gid in g_ids:
            amt = float(gift_amount_by_id.get(gid, 0.0))
            grp = (gift_group_by_id.get(gid) or "").strip()
            a24 = amt if grp == "2024 Gifts" else 0.0
            a25 = amt if grp == "2025 Gifts" else 0.0
            mapped_class = (gift_mapped_class_by_id.get(gid) or "").strip()

            rows.append({
                "region": region,
                "mapped_class": mapped_class,
                "amount": amt,
                "additions_2024": a24,
                "additions_2025": a25,
            })

    cols = ["region", "mapped_class", "amount", "additions_2024", "additions_2025"]
    detail = pd.DataFrame(rows)
    if detail.empty:
        return pd.DataFrame(columns=cols)

    # 1) Base grain: one row per (region, mapped_class)
    by_region_class = (
        detail
        .groupby(["region", "mapped_class"], dropna=False, as_index=False)[["amount","additions_2024","additions_2025"]]
        .sum()
    )

    # 2) Per-region total (mapped_class="Total")
    per_region_total = (
        by_region_class
        .groupby("region", as_index=False)[["amount","additions_2024","additions_2025"]]
        .sum()
        .assign(mapped_class="Total")
    )

    # 3) "Total" region broken out by mapped_class
    total_region_by_class = (
        by_region_class
        .groupby("mapped_class", as_index=False)[["amount","additions_2024","additions_2025"]]
        .sum()
        .assign(region="Total")
    )[cols]

    # 4) Single grand total row (region="Total", mapped_class="Total")
    grand_total = (
        by_region_class[["amount","additions_2024","additions_2025"]]
        .sum()
        .to_frame().T
        .assign(region="Total", mapped_class="Total")
    )[cols]

    # Combine all parts
    final_df = pd.concat(
        [by_region_class, per_region_total, total_region_by_class, grand_total],
        ignore_index=True
    )[cols]

    # Order mapped_class so "Total" appears last within each region
    mapped_classes = [mc for mc in final_df["mapped_class"].dropna().unique() if mc != "Total"]
    mapped_classes.sort()
    mapped_classes.append("Total")
    final_df["mapped_class"] = pd.Categorical(final_df["mapped_class"], categories=mapped_classes, ordered=True)

    # Push the "Total" region to the bottom; stable sort by region then mapped_class
    final_df["_is_total_region"] = final_df["region"].eq("Total")
    final_df = (
        final_df
        .sort_values(["region", "mapped_class"], kind="stable")
        .sort_values(["_is_total_region"], kind="stable")
        .drop(columns="_is_total_region")
        .reset_index(drop=True)
    )

    return final_df


# === usage ===
region_rev_breakdown = summarize_region_gifts(pledges_df)

# Quick lookup for gift amount by id AND by group/year
gift_group_by_id = dict(zip(all_gifts_df["gift_id"], all_gifts_df["group_title"]))

def balances_by_region(pledges_df):
    def is_3yr(s): return bool(s) and ("3-year" in s.lower() or "3 year" in s.lower())
    def is_one_time(s): return str(s).strip().lower() == "one-time"

    rows = []
    for _, p in pledges_df.iterrows():
        group = (p["group_title"] or "").strip()
        ct = (p["commitment_type"] or "").strip()
        total = float(p["total_commitment"] or 0.0)
        part = (total/3.0) if is_3yr(ct) else total

        # Commitments
        c2024 = part if group == "2024" else 0.0
        c2025 = 0.0 if (is_one_time(ct) and group == "2024") else part
        ctotal = total

        # Gifts by labeled group
        g2024 = g2025 = gsum = 0.0
        for gid in p["linked_gift_ids"] or []:
            amt = gift_amount_by_id.get(gid, 0.0)
            grp = (gift_group_by_id.get(gid) or "").strip()
            gsum += amt
            if grp == "2024 Gifts":
                g2024 += amt
            elif grp == "2025 Gifts":
                g2025 += amt

        # First, apply 2024 gifts to 2024 commitment
        rem_2024_after_2024gifts = max(c2024 - g2024, 0.0)

        # Spill 2025 gifts into 2024 up to remaining 2024 balance
        spill_to_2024 = min(g2025, rem_2024_after_2024gifts)
        g2025_after_spill = g2025 - spill_to_2024

        # Final balances
        b2024 = max(c2024 - (g2024 + spill_to_2024), 0.0)
        b2025 = max(c2025 - g2025_after_spill, 0.0)
        btotal = max(ctotal - gsum, 0.0)  # total balance unaffected by spill logic

        rows.append({
            "region": p["region"],
            "balance_2024": b2024,
            "balance_2025": b2025,
            "balance_total": btotal
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    agg = out.groupby("region", dropna=False, as_index=False).sum(numeric_only=True)
    for col in ["balance_2024", "balance_2025", "balance_total"]:
        agg[f"{col}_restricted"] = agg[col] * 0.70
        agg[f"{col}_unrestricted"] = agg[col] * 0.30
    return agg

region_balances = balances_by_region(pledges_df)

