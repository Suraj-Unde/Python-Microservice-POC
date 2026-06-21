import unittest

from common.events import create_event, create_rejected_event, validate_event


class EventContractTests(unittest.TestCase):
    def test_create_event_builds_valid_envelope(self):
        event = create_event("ORDER_CREATED", {"order_id": 1}, "correlation-1")

        self.assertIs(validate_event(event, {"ORDER_CREATED"}), event)
        self.assertEqual(event["schema_version"], 1)
        self.assertEqual(event["payload"]["order_id"], 1)

    def test_validate_event_rejects_missing_payload(self):
        event = create_event("ORDER_CREATED", {"order_id": 1}, "correlation-1")
        del event["payload"]

        with self.assertRaisesRegex(ValueError, "missing fields"):
            validate_event(event)

    def test_validate_event_rejects_unknown_version(self):
        event = create_event("ORDER_CREATED", {"order_id": 1}, "correlation-1")
        event["schema_version"] = 2

        with self.assertRaisesRegex(ValueError, "unsupported schema version"):
            validate_event(event)

    def test_validate_event_rejects_invalid_identifier(self):
        event = create_event("ORDER_CREATED", {"order_id": 1}, "correlation-1")
        event["event_id"] = "not-a-uuid"

        with self.assertRaisesRegex(ValueError, "event_id must be a UUID"):
            validate_event(event)

    def test_rejected_event_preserves_failure_context(self):
        failed = create_event("UNKNOWN", {}, "correlation-1")
        rejected = create_rejected_event("orders", failed, "bad contract")

        validate_event(rejected, {"EVENT_REJECTED"})
        self.assertEqual(rejected["causation_id"], failed["event_id"])
        self.assertEqual(rejected["payload"]["source_topic"], "orders")


if __name__ == "__main__":
    unittest.main()
