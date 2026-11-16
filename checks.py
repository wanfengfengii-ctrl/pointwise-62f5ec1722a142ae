import unittest
from checkpoint import Journal
from joiner import Joiner
from lab import BOUNDARIES, boundary_case, observe
from receiver import Receiver
from samples import EXPECTED
from sources import Inbox


class BusinessChecks(unittest.TestCase):
    def test_normal_and_paused(self):
        report = observe()
        for name in ("uninterrupted", "paused_reference"):
            self.assertEqual(report[name]["rows"], EXPECTED)
            self.assertEqual(report[name]["state"]["active"], {"L": {}, "R": {}})
            self.assertEqual(report[name]["state"]["watermarks"], {"L": 100, "R": 100})
            self.assertLessEqual(report[name]["peak_ingress_bytes"], 256)
        self.assertGreater(report["paused_reference"]["blocked_offers"], 0)
        payload, complete = report["manifest"]
        self.assertTrue(complete)
        self.assertEqual(payload["positions"], {"L": 3, "R": 6})
        self.assertEqual(payload["state"]["watermarks"], {"L": -10**18, "R": 23})

    def test_restart_boundaries(self):
        for boundary in BOUNDARIES:
            with self.subTest(boundary=boundary):
                answer = boundary_case(boundary)
                self.assertEqual(answer["recovered"]["rows"], EXPECTED)
                self.assertEqual(len(answer["recovered"]["rows"]), len(EXPECTED))
                self.assertEqual(answer["recovered"]["state"]["active"], {"L": {}, "R": {}})

    def test_strict_watermark(self):
        operator = Joiner()
        self.assertEqual(operator.accept("L", {"kind": "row", "id": "x", "t": 10}), [])
        self.assertEqual(operator.accept("R", {"kind": "wm", "t": 12}), [])
        self.assertEqual(operator.accept("R", {"kind": "row", "id": "y", "t": 12}),
                         [["pair", "x", "y"]])
        self.assertEqual(operator.accept("R", {"kind": "wm", "t": 13}), [])
        isolated = Joiner()
        isolated.accept("L", {"kind": "row", "id": "z", "t": 10})
        self.assertEqual(isolated.accept("R", {"kind": "wm", "t": 13}),
                         [["unmatched", "L", "z"]])

    def test_receiver_lost_ack_and_conflict(self):
        receiver = Receiver()
        try:
            receiver.stage("b", [["pair", "a", "b"]])
            with self.assertRaises(ConnectionError):
                receiver.commit("b", lose_response=True)
            receiver.stage("b", [["pair", "a", "b"]])
            receiver.commit("b")
            self.assertEqual(receiver.rows(), [["pair", "a", "b"]])
            with self.assertRaisesRegex(ValueError, "BATCH_CONTENT_CONFLICT"):
                receiver.stage("b", [])
        finally:
            receiver.close()

    def test_borrowed_envelope_remains_charged(self):
        inbox = Inbox(100)
        inbox.offer({"payload": "x" * 40})
        token, message = inbox.take()
        self.assertGreater(inbox.used, 0)
        with self.assertRaises(BufferError):
            inbox.offer({"payload": "x" * 40})
        inbox.release(token)
        self.assertEqual(inbox.used, 0)
        self.assertEqual(message["payload"], "x" * 40)


if __name__ == "__main__":
    unittest.main()
