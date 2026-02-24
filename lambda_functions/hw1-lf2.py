import os, json, random
from decimal import Decimal
import boto3
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.session import Session
from botocore.httpsession import URLLib3Session

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
QUEUE_URL = os.environ["SQS_QUEUE_URL"]
SES_SENDER = os.environ["SES_SENDER"]
OPENSEARCH_ENDPOINT = os.environ["OPENSEARCH_ENDPOINT"].rstrip("/")
DDB_TABLE = os.getenv("DDB_TABLE", "yelp-restaurants")
OS_INDEX = os.getenv("OS_INDEX", "restaurants")

TEST_RECIPIENT = os.getenv("TEST_RECIPIENT")

sqs = boto3.client("sqs", region_name=AWS_REGION)
ses = boto3.client("ses", region_name=AWS_REGION)
ddb = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DDB_TABLE)

_session = Session()
_credentials = _session.get_credentials()
_signer = SigV4Auth(_credentials, "es", AWS_REGION)
_http = URLLib3Session()

def _signed_request(method: str, url: str, body: bytes = b"", headers=None):
    headers = headers or {}
    req = AWSRequest(method=method, url=url, data=body, headers=headers)
    _signer.add_auth(req)
    prepared = req.prepare()

    resp = _http.send(prepared)
    status = resp.status_code
    text = resp.content.decode("utf-8") if resp.content else ""
    return status, text

def query_opensearch_ids(cuisine: str, size: int = 50):
    payload = {
        "size": size,
        "_source": ["RestaurantID"],
        "query": {"term": {"Cuisine": cuisine}}
    }
    url = f"{OPENSEARCH_ENDPOINT}/{OS_INDEX}/_search"
    status, text = _signed_request("POST", url, body=json.dumps(payload).encode("utf-8"),
                               headers={"Content-Type": "application/json"})
    if status >= 300:
        raise RuntimeError(f"OpenSearch query failed {status}: {text}")
    data = json.loads(text)
    hits = data.get("hits", {}).get("hits", [])

    ids = []
    for h in hits:
        src = h.get("_source", {})
        rid = src.get("RestaurantID")
        if rid:
            ids.append(rid)
    return ids

def batch_get_restaurants(ids):
    keys = [{"businessId": rid} for rid in ids]
    resp = ddb.meta.client.batch_get_item(
        RequestItems={DDB_TABLE: {"Keys": keys}}
    )
    items = resp.get("Responses", {}).get(DDB_TABLE, [])
    by_id = {it["businessId"]: it for it in items if "businessId" in it}
    return [by_id.get(rid) for rid in ids if rid in by_id]

def format_item(it):
    name = it.get("name", "Unknown")
    addr = it.get("address", "Unknown address")
    rating = it.get("rating", "N/A")
    reviews = it.get("review_count", "N/A")
    return f"- {name} | {addr} | rating {rating} ({reviews} reviews)"

def lambda_handler(event, context):
    resp = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    messages = resp.get("Messages", [])
    if not messages:
        return {"statusCode": 200, "body": "No messages in queue."}

    msg = messages[0]
    body = json.loads(msg["Body"])

    cuisine = str(body.get("cuisine", "")).lower().strip()
    party = str(body.get("partySize", "N/A")).strip()
    time_ = str(body.get("diningTime", "N/A")).strip()
    location = str(body.get("location", "Manhattan")).strip()

    to_email = (TEST_RECIPIENT or body.get("email", "")).strip()

    # OpenSearch: get candidate IDs
    ids = query_opensearch_ids(cuisine, size=50)
    if len(ids) < 3:
        raise RuntimeError(f"Not enough restaurants found for cuisine={cuisine}. Got {len(ids)} IDs")

    chosen = random.sample(ids, 3)

    # DynamoDB: get full details
    items = batch_get_restaurants(chosen)
    if len(items) < 3:
        pass

    lines = [format_item(it) for it in items if it]

    email_text = (
        f"Hello!\n\nHere are your {cuisine} suggestions in {location} "
        f"for {party} people at {time_}:\n\n"
        + "\n".join(lines) +
        "\n\nEnjoy your meal!"
    )

    # Send email via SES
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [to_email]},
        Message={
            "Subject": {"Data": "Your Dining Concierge Suggestions"},
            "Body": {"Text": {"Data": email_text}},
        },
    )

    # Delete message only after successful email
    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])

    return {"statusCode": 200, "body": f"Sent recommendations to {to_email}."}