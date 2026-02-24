import os
import time
import json
import random
import datetime
from typing import Dict, List, Tuple
from decimal import Decimal

import requests
import boto3
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.session import Session


# ----------------------------
# Config
# ----------------------------
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

YELP_API_KEY = os.environ["YELP_API_KEY"]
OPENSEARCH_ENDPOINT = os.environ["OPENSEARCH_ENDPOINT"].rstrip("/")
DDB_TABLE = os.getenv("DDB_TABLE", "yelp-restaurants")
OS_INDEX = os.getenv("OS_INDEX", "restaurants")

CUISINES = [c.strip().lower() for c in os.getenv("CUISINES", "mexican,japanese,korean,italian,chinese").split(",") if c.strip()]

# Per cuisine target count
PER_CUISINE_TARGET = int(os.getenv("PER_CUISINE_TARGET", "200"))

LOCATION = os.getenv("LOCATION", "Manhattan, NY")
LIMIT = 50
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "0.2"))  # gentle throttling


# ----------------------------
# AWS clients
# ----------------------------
ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = ddb.Table(DDB_TABLE)

session = Session()
credentials = session.get_credentials()
sigv4 = SigV4Auth(credentials, "es", AWS_REGION)


# ----------------------------
# Helpers
# ----------------------------
def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def to_decimal(x):
    if x is None:
        return None
    # avoid Decimal(float) due to precision issues; convert via string
    return Decimal(str(x))

def yelp_search(cuisine: str, offset: int) -> Dict:
    url = "https://api.yelp.com/v3/businesses/search"
    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
    params = {
        "location": LOCATION,
        "categories": "restaurants",
        "term": cuisine,
        "limit": LIMIT,
        "offset": offset,
        "sort_by": "best_match",
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def ddb_put_business(b: Dict, cuisine: str) -> None:
    # Normalize fields for DynamoDB. Keep only what you need + timestamp.
    item = {
        "businessId": b.get("id"),
        "name": b.get("name"),
        "address": ", ".join(b.get("location", {}).get("display_address", []) or []),
        "coordinates": {
            "latitude": to_decimal(b.get("coordinates", {}).get("latitude")),
            "longitude": to_decimal(b.get("coordinates", {}).get("longitude")),
        },
        "review_count": int(b.get("review_count")) if b.get("review_count") is not None else None,
        "rating": to_decimal(b.get("rating")),
        "zip_code": b.get("location", {}).get("zip_code"),
        "cuisine": cuisine,
        "insertedAtTimestamp": now_iso(),
    }
    # Basic guard
    if not item["businessId"]:
        return
    table.put_item(Item=item)


def sign_and_request(method: str, url: str, body: bytes = b"", headers: Dict[str, str] = None) -> requests.Response:
    headers = headers or {}
    req = AWSRequest(method=method, url=url, data=body, headers=headers)
    sigv4.add_auth(req)
    prepared = req.prepare()

    r = requests.request(
        method=method,
        url=prepared.url,
        headers=dict(prepared.headers),
        data=body,
        timeout=60,
    )
    return r


def opensearch_bulk(docs: List[Dict]) -> None:
    """
    Bulk index documents into OS_INDEX.
    Each doc should include RestaurantID and Cuisine.
    """
    if not docs:
        return

    lines = []
    for d in docs:
        lines.append(json.dumps({"index": {"_index": OS_INDEX}}))
        lines.append(json.dumps(d))
    payload = ("\n".join(lines) + "\n").encode("utf-8")

    url = f"{OPENSEARCH_ENDPOINT}/_bulk"
    r = sign_and_request(
        "POST",
        url,
        body=payload,
        headers={"Content-Type": "application/x-ndjson"},
    )
    if r.status_code >= 300:
        raise RuntimeError(f"OpenSearch bulk failed: {r.status_code} {r.text}")

    resp = r.json()
    if resp.get("errors"):
        # show first few errors
        items = resp.get("items", [])[:5]
        raise RuntimeError(f"OpenSearch bulk had errors. Sample: {items}")


def opensearch_count() -> int:
    url = f"{OPENSEARCH_ENDPOINT}/{OS_INDEX}/_count"
    r = sign_and_request("GET", url)
    if r.status_code >= 300:
        raise RuntimeError(f"OpenSearch count failed: {r.status_code} {r.text}")
    return int(r.json().get("count", 0))


# ----------------------------
# Main ingest
# ----------------------------
def ingest() -> None:
    seen_ids = set()
    total_ddb = 0
    total_os = 0

    print(f"Region={AWS_REGION}")
    print(f"DDB table={DDB_TABLE}")
    print(f"OS endpoint={OPENSEARCH_ENDPOINT}, index={OS_INDEX}")
    print(f"Location={LOCATION}")
    print(f"Cuisines={CUISINES}, target per cuisine={PER_CUISINE_TARGET}")

    bulk_buf = []
    BULK_FLUSH_EVERY = 500

    for cuisine in CUISINES:
        collected = 0
        offset = 0

        print(f"\n=== Cuisine: {cuisine} ===")
        while collected < PER_CUISINE_TARGET:
            try:
                data = yelp_search(cuisine, offset)
            except requests.exceptions.HTTPError as e:
                print(f"Yelp HTTPError at cuisine={cuisine} offset={offset}: {e}")
                break
            businesses = data.get("businesses", [])
            if not businesses:
                print("No more results from Yelp.")
                break

            for b in businesses:
                bid = b.get("id")
                if not bid or bid in seen_ids:
                    continue
                seen_ids.add(bid)

                ddb_put_business(b, cuisine)
                total_ddb += 1

                bulk_buf.append({"RestaurantID": bid, "Cuisine": cuisine})
                total_os += 1

                collected += 1
                if collected >= PER_CUISINE_TARGET:
                    break

                if len(bulk_buf) >= BULK_FLUSH_EVERY:
                    opensearch_bulk(bulk_buf)
                    bulk_buf.clear()
                    print(f"Flushed {BULK_FLUSH_EVERY} to OpenSearch. total_os={total_os}")

            offset += LIMIT
            if offset >= 1000:
                print("Reached Yelp offset limit boundary (>=1000). Stopping this cuisine.")
                break

            time.sleep(SLEEP_SEC)

        print(f"Collected for {cuisine}: {collected}")

    # final flush
    if bulk_buf:
        opensearch_bulk(bulk_buf)
        print(f"Final flush {len(bulk_buf)} to OpenSearch.")
        bulk_buf.clear()

    print("\nDone.")
    print(f"Total written to DynamoDB: {total_ddb}")
    print(f"Total written to OpenSearch: {total_os}")
    try:
        print(f"OpenSearch index count now: {opensearch_count()}")
    except Exception as e:
        print(f"Could not count OpenSearch docs: {e}")


if __name__ == "__main__":
    ingest()