import os
import json
import boto3

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

LEX_BOT_ID = os.environ["LEX_BOT_ID"]
LEX_BOT_ALIAS_ID = os.environ["LEX_BOT_ALIAS_ID"]
LEX_LOCALE_ID = os.getenv("LEX_LOCALE_ID", "en_US")


lex = boto3.client("lexv2-runtime", region_name=AWS_REGION)

_NOTICE_SHOWN = set()

def lambda_handler(event, context):
    # Parse incoming message from your starter frontend
    body = event.get("body") or "{}"
    if isinstance(body, str):
        body = json.loads(body)

    text = ""
    try:
        text = body["messages"][0]["unstructured"]["text"]
    except Exception:
        text = body.get("message", "") or ""

    if not text:
        return _resp("Please type something.")

    # Use a stable Lex session id provided by the frontend
    session_id = (body.get("sessionId") or "web-session")
    if not isinstance(session_id, str):
        session_id = str(session_id)
    session_id = session_id.strip() or "web-session"

    r = lex.recognize_text(
        botId=LEX_BOT_ID,
        botAliasId=LEX_BOT_ALIAS_ID,
        localeId=LEX_LOCALE_ID,
        sessionId=session_id,
        text=text
    )

    # Convert Lex response -> starter expected format
    lex_msgs = r.get("messages", [])
    out_messages = []
    if lex_msgs:
        for m in lex_msgs:
            if m.get("contentType") == "PlainText":
                out_messages.append({
                    "type": "unstructured",
                    "unstructured": {"text": m.get("content", "")}
                })

        # Extra credit
        # Gate on Lex being in DiningSuggestionsIntent and eliciting DiningTime
        session_state = r.get("sessionState", {}) or {}
        session_attrs = session_state.get("sessionAttributes") or {}
        reused_notice = session_attrs.get("reused_notice")

        intent_name = (session_state.get("intent") or {}).get("name")
        dialog_action = session_state.get("dialogAction") or {}
        slot_to_elicit = dialog_action.get("slotToElicit")

        if (
            reused_notice
            and session_id not in _NOTICE_SHOWN
            and intent_name == "DiningSuggestionsIntent"
            and slot_to_elicit == "DiningTime"
        ):
            out_messages.insert(0, {
                "type": "unstructured",
                "unstructured": {"text": reused_notice}
            })
            _NOTICE_SHOWN.add(session_id)
    else:
        out_messages.append({
            "type": "unstructured",
            "unstructured": {"text": "Sorry, I didn't get that."}
        })

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        },
        "body": json.dumps({"messages": out_messages})
    }

def _resp(msg: str):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        },
        "body": json.dumps({"messages":[
            {
            "type":"unstructured",
             "unstructured":{"text": msg}
            }
        ]})
    }