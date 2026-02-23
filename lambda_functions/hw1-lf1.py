import json
import os
import re
import uuid
from datetime import datetime, timezone

import boto3

sqs = boto3.client("sqs")
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

ALLOWED_CUISINES = {"chinese", "japanese", "italian", "mexican", "indpak", "korean", "thai", "vietnamese"}

def slot_value(slots, name):
    slot = (slots or {}).get(name)
    if not slot:
        return None
    val = slot.get("value", {})
    return val.get("interpretedValue") or val.get("originalValue")


def close(intent_name, slots, message):
    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {"name": intent_name, "slots": slots, "state": "Fulfilled"},
        },
        "messages": [{"contentType": "PlainText", "content": message}],
    }


def elicit(intent_name, slots, slot_to_elicit, message):
    return {
        "sessionState": {
            "dialogAction": {"type": "ElicitSlot", "slotToElicit": slot_to_elicit},
            "intent": {"name": intent_name, "slots": slots, "state": "InProgress"},
        },
        "messages": [{"contentType": "PlainText", "content": message}],
    }


def delegate(intent_name, slots):
    return {
        "sessionState": {
            "dialogAction": {"type": "Delegate"},
            "intent": {"name": intent_name, "slots": slots, "state": "InProgress"},
        }
    }


def lambda_handler(event, context):
    intent = event["sessionState"]["intent"]
    intent_name = intent["name"]
    slots = intent.get("slots", {})

    if intent_name == "GreetingIntent":
        return close(intent_name, slots, "Hi there, how can I help?")
    if intent_name == "ThankYouIntent":
        return close(intent_name, slots, "You're welcome.")

    if intent_name != "DiningSuggestionsIntent":
        return close(intent_name, slots, "Sorry, I couldn't understand that.")

    location = (slot_value(slots, "Location") or "").strip()
    cuisine = (slot_value(slots, "Cuisine") or "").strip()
    dining_time = (slot_value(slots, "DiningTime") or "").strip()
    party_size = (slot_value(slots, "NumberOfPeople") or "").strip()
    email = (slot_value(slots, "Email") or "").strip()

    if location and ("manhattan" not in location.lower() and "new york" not in location.lower()):
        return elicit(intent_name, slots, "Location", "Sorry, I can only support Manhattan. Please provide a Manhattan location.")

    if cuisine and cuisine.lower() not in ALLOWED_CUISINES:
        return elicit(intent_name, slots, "Cuisine", "Please choose one of: Chinese, Japanese, Italian, Mexican, Korean, Thai, Indpak, Vietnamese.")

    if party_size and (not party_size.isdigit() or int(party_size) < 1 or int(party_size) > 20):
        return elicit(intent_name, slots, "NumberOfPeople", "Please provide a party size between 1 and 20.")

    if email and not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        return elicit(intent_name, slots, "Email", "Please provide a valid email address.")

    required = [location, cuisine, dining_time, party_size, email]
    if not all(required):
        return delegate(intent_name, slots)

    payload = {
        "location": location,
        "cuisine": cuisine.lower(),
        "diningTime": dining_time,
        "partySize": party_size,
        "email": email,
        "requestId": str(uuid.uuid4()),
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }

    sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(payload))

    return close(
        intent_name,
        slots,
        "You’re all set. I will send restaurant suggestions to your email shortly.",
    )
