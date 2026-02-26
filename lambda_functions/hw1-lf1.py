import json
import os
import re
import uuid
from datetime import datetime, timezone

import boto3

sqs = boto3.client("sqs")
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

# Extra credit: remember last-used preferences
PREF_TABLE_NAME = os.getenv("PREF_TABLE", "hw1-userprefs")
ddb = boto3.resource("dynamodb")
pref_table = ddb.Table(PREF_TABLE_NAME)

ALLOWED_CUISINES = {"chinese", "japanese", "italian", "mexican", "indpak", "korean", "thai", "vietnamese"}

def slot_value(slots, name):
    slot = (slots or {}).get(name)
    if not slot:
        return None
    val = slot.get("value", {})
    return val.get("interpretedValue") or val.get("originalValue")


def make_slot(v: str):
    """Create a Lex V2 slot object from a plain string."""
    return {
        "value": {
            "originalValue": v,
            "interpretedValue": v,
            "resolvedValues": [v],
        }
    }


def get_prefs(session_id: str):
    """Fetch last prefs for this sessionId from DynamoDB."""
    try:
        resp = pref_table.get_item(Key={"sessionId": session_id})
        return resp.get("Item")
    except Exception as e:
        # Don't break the dialog if prefs lookup fails
        print("prefs get_item failed", e)
        return None


def save_prefs(session_id: str, location: str, cuisine: str):
    """Upsert last prefs for this sessionId into DynamoDB."""
    try:
        pref_table.put_item(
            Item={
                "sessionId": session_id,
                "lastLocation": location,
                "lastCuisine": cuisine,
                "updatedAt": datetime.now(timezone.utc).isoformat(),
            }
        )
    except Exception as e:
        print("prefs put_item failed", e)

def close(intent_name, slots, message, session_attributes=None):
    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {"name": intent_name, "slots": slots, "state": "Fulfilled"},
            "sessionAttributes": session_attributes or {},
        },
        "messages": [{"contentType": "PlainText", "content": message}],
    }


def elicit(intent_name, slots, slot_to_elicit, message, session_attributes=None):
    return {
        "sessionState": {
            "dialogAction": {"type": "ElicitSlot", "slotToElicit": slot_to_elicit},
            "intent": {"name": intent_name, "slots": slots, "state": "InProgress"},
            "sessionAttributes": session_attributes or {},
        },
        "messages": [{"contentType": "PlainText", "content": message}],
    }


def delegate(intent_name, slots, session_attributes=None):
    return {
        "sessionState": {
            "dialogAction": {"type": "Delegate"},
            "intent": {"name": intent_name, "slots": slots, "state": "InProgress"},
            "sessionAttributes": session_attributes or {},
        }
    }


# Helper: delegate with an informational message to the user
def delegate_with_message(intent_name, slots, message, session_attributes=None):
    return {
        "sessionState": {
            "dialogAction": {"type": "Delegate"},
            "intent": {"name": intent_name, "slots": slots, "state": "InProgress"},
            "sessionAttributes": session_attributes or {},
        },
        "messages": [{"contentType": "PlainText", "content": message}],
    }


def lambda_handler(event, context):
    intent = event["sessionState"]["intent"]
    intent_name = intent["name"]
    slots = intent.get("slots", {})
    session_attrs = event.get("sessionState", {}).get("sessionAttributes") or {}

    invocation_source = event.get("invocationSource")  # DialogCodeHook or FulfillmentCodeHook
    session_id = event.get("sessionId")

    if intent_name == "GreetingIntent":
        return close(intent_name, slots, "Hi there, how can I help?", session_attrs)
    if intent_name == "ThankYouIntent":
        return close(intent_name, slots, "You're welcome.", session_attrs)

    if intent_name != "DiningSuggestionsIntent":
        return close(intent_name, slots, "Sorry, I couldn't understand that.", session_attrs)

    # Extra credit: auto-fill last-used Location/Cuisine and inform the user
    if invocation_source == "DialogCodeHook" and session_id:
        pref = get_prefs(session_id)
        filled_location = False
        filled_cuisine = False

        if pref:
            if not slot_value(slots, "Location") and pref.get("lastLocation"):
                slots["Location"] = make_slot(pref["lastLocation"])
                filled_location = True
            if not slot_value(slots, "Cuisine") and pref.get("lastCuisine"):
                slots["Cuisine"] = make_slot(pref["lastCuisine"])
                filled_cuisine = True

        if filled_location or filled_cuisine:
            reused_loc = slot_value(slots, "Location") or ""
            reused_cui = slot_value(slots, "Cuisine") or ""
            parts = []
            if filled_location:
                parts.append(f"location: {reused_loc}")
            if filled_cuisine:
                parts.append(f"cuisine: {reused_cui}")
            msg = "Reusing your last preferences (" + ", ".join(parts) + ")."
            session_attrs["reused_notice"] = msg
            return delegate_with_message(intent_name, slots, msg, session_attrs)

        pass

    location = (slot_value(slots, "Location") or "").strip()
    cuisine = (slot_value(slots, "Cuisine") or "").strip()
    dining_time = (slot_value(slots, "DiningTime") or "").strip()
    party_size = (slot_value(slots, "NumberOfPeople") or "").strip()
    email = (slot_value(slots, "Email") or "").strip()

    if location and ("manhattan" not in location.lower() and "new york" not in location.lower()):
        return elicit(intent_name, slots, "Location", "Sorry, I can only support Manhattan. Please provide a valid location.", session_attrs)

    if cuisine and cuisine.lower() not in ALLOWED_CUISINES:
        return elicit(intent_name, slots, "Cuisine", "Please choose one of: Chinese, Japanese, Italian, Mexican, Korean, Thai, Indpak, Vietnamese.", session_attrs)

    if party_size and (not party_size.isdigit() or int(party_size) < 1 or int(party_size) > 20):
        return elicit(intent_name, slots, "NumberOfPeople", "Please provide a party size between 1 and 20.", session_attrs)

    if email and not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        return elicit(intent_name, slots, "Email", "Please provide a valid email address.", session_attrs)

    required = [location, cuisine, dining_time, party_size, email]
    if not all(required):
        return delegate(intent_name, slots, session_attrs)

    # Enqueue only once per session turn to avoid duplicates
    if session_attrs.get("enqueued") == "1":
        return close(
            intent_name,
            slots,
            "You’re all set. I will send restaurant suggestions to your email shortly.",
            session_attrs,
        )

    # Mark as enqueued in the Lex session
    session_attrs["enqueued"] = "1"

    payload = {
        "location": location,
        "cuisine": cuisine.lower(),
        "diningTime": dining_time,
        "partySize": party_size,
        "email": email,
        "requestId": str(uuid.uuid4()),
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }

    # persist last-used preferences for next time
    if session_id:
        save_prefs(session_id, location.lower(), cuisine.lower())

    sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(payload))

    return close(
        intent_name,
        slots,
        "You’re all set. I will send restaurant suggestions to your email shortly.",
        session_attrs,
    )
