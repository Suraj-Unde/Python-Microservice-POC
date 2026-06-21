from datetime import datetime, timezone
from uuid import UUID, uuid4


REQUIRED_FIELDS = {
    "event_id",
    "event_type",
    "schema_version",
    "occurred_at",
    "correlation_id",
    "payload",
}


def create_event(event_type, payload, correlation_id, causation_id=None):
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": correlation_id,
        "causation_id": causation_id,
        "payload": payload,
    }


def validate_event(event, accepted_types=None):
    if not isinstance(event, dict):
        raise ValueError("event must be an object")
    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        raise ValueError(f"event missing fields: {', '.join(sorted(missing))}")
    if event["schema_version"] != 1:
        raise ValueError(f"unsupported schema version: {event['schema_version']}")
    try:
        UUID(event["event_id"])
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("event_id must be a UUID") from exc
    if not isinstance(event["event_type"], str) or not event["event_type"]:
        raise ValueError("event_type must be a non-empty string")
    try:
        datetime.fromisoformat(event["occurred_at"])
    except (TypeError, ValueError) as exc:
        raise ValueError("occurred_at must be an ISO-8601 timestamp") from exc
    if event.get("causation_id") is not None:
        try:
            UUID(event["causation_id"])
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("causation_id must be a UUID") from exc
    if not isinstance(event["payload"], dict):
        raise ValueError("event payload must be an object")
    if accepted_types and event["event_type"] not in accepted_types:
        raise ValueError(f"unsupported event type: {event['event_type']}")
    return event


def create_rejected_event(source_topic, failed_event, error):
    return create_event(
        "EVENT_REJECTED",
        {
            "source_topic": source_topic,
            "error": str(error),
            "failed_event": failed_event,
        },
        failed_event.get("correlation_id") if isinstance(failed_event, dict) else None,
        causation_id=failed_event.get("event_id") if isinstance(failed_event, dict) else None,
    )
