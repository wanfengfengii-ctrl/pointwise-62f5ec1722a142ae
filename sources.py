import json
from collections import deque


class Sources:
    """Immutable external logs; one forward-only session per run."""
    def __init__(self, logs, positions=None):
        self.logs = logs
        self.positions = dict(positions or {"L": 0, "R": 0})
        for side in ("L", "R"):
            wm, ids, barriers = -10**18, set(), 0
            for event in logs[side]:
                if event["kind"] == "row":
                    if event["id"] in ids or event["t"] < wm:
                        raise ValueError("invalid source row")
                    ids.add(event["id"])
                elif event["kind"] == "wm":
                    if event["t"] < wm:
                        raise ValueError("watermark regression")
                    wm = event["t"]
                elif event == {"kind": "barrier", "checkpoint": "c1"}:
                    barriers += 1
                else:
                    raise ValueError("invalid source event")
            if barriers != 1:
                raise ValueError("one barrier per source required")
            if not 0 <= self.positions[side] <= len(logs[side]):
                raise ValueError("invalid restore position")

    def peek(self, side):
        pos = self.positions[side]
        if pos == len(self.logs[side]):
            return None
        return {"side": side, "offset": pos, "event": self.logs[side][pos]}

    def advance(self, side):
        self.positions[side] += 1

    def barrier_positions(self):
        return {s: next(i + 1 for i, e in enumerate(self.logs[s])
                        if e["kind"] == "barrier") for s in ("L", "R")}


class Inbox:
    """Charges queued AND borrowed envelopes until release."""
    def __init__(self, budget=256):
        self.budget, self.used, self.peak = budget, 0, 0
        self.queue, self.held, self.serial = deque(), {}, 0

    def offer(self, envelope):
        raw = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()
        if self.used + len(raw) > self.budget:
            raise BufferError("INGRESS_BUDGET")
        self.serial += 1
        token = self.serial
        self.held[token] = (raw, len(raw))
        self.queue.append(token)
        self.used += len(raw)
        self.peak = max(self.peak, self.used)

    def take(self):
        token = self.queue.popleft()
        return token, json.loads(self.held[token][0])

    def release(self, token):
        _, size = self.held.pop(token)
        self.used -= size
