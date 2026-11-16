import argparse
import json
from checkpoint import BoundaryStop, Journal, quiescent, restore
from joiner import Joiner
from receiver import Receiver
from samples import LOGS, SCHEDULE
from sources import Inbox, Sources

BOUNDARIES = ["before_prepare", "after_prepare", "after_stage",
              "after_sink_commit", "after_complete"]


def deliver(sources, operator, pending, inbox, side, trace):
    envelope = sources.peek(side)
    if envelope is None:
        raise ValueError("schedule exhausted source")
    inbox.offer(envelope)
    sources.advance(side)
    token, message = inbox.take()
    try:
        event = message["event"]
        trace.append({"side": side, "offset": message["offset"], "event": event})
        if event["kind"] != "barrier":
            pending.extend(operator.accept(side, event))
        return event["kind"]
    finally:
        inbox.release(token)


def finish(sources, operator, pending, receiver, inbox, trace):
    while any(sources.peek(s) is not None for s in ("L", "R")):
        for side in ("L", "R"):
            if sources.peek(side) is not None:
                deliver(sources, operator, pending, inbox, side, trace)
    receiver.stage("tail", pending)
    receiver.commit("tail")
    return {"rows": receiver.rows(), "state": operator.snapshot(),
            "peak_ingress_bytes": inbox.peak, "trace": trace}


def reference(receiver, journal, fault=None):
    sources, operator, pending = Sources(LOGS), Joiner(), []
    inbox, trace, blocked, barriers = Inbox(), [], [], set()
    checkpoint_done = False
    for side in SCHEDULE:
        if side in barriers and not checkpoint_done:
            blocked.append(side)
            trace.append({"blocked_offer": side})
            continue
        kind = deliver(sources, operator, pending, inbox, side, trace)
        if kind == "barrier" and not checkpoint_done:
            barriers.add(side)
            if len(barriers) == 2:
                pending = quiescent(sources, operator, pending, receiver, journal, fault)
                checkpoint_done = True
                for deferred in blocked:
                    deliver(sources, operator, pending, inbox, deferred, trace)
    if not checkpoint_done:
        raise ValueError("schedule did not reach checkpoint")
    answer = finish(sources, operator, pending, receiver, inbox, trace)
    answer["blocked_offers"] = len(blocked)
    return answer


def uninterrupted():
    receiver = Receiver()
    sources, operator, pending = Sources(LOGS), Joiner(), []
    inbox, trace = Inbox(), []
    try:
        for side in SCHEDULE:
            deliver(sources, operator, pending, inbox, side, trace)
        return finish(sources, operator, pending, receiver, inbox, trace)
    finally:
        receiver.close()


def resumed(receiver, journal):
    sources, operator, pending = restore(LOGS, receiver, journal)
    return finish(sources, operator, pending, receiver, Inbox(), [])


def observe():
    receiver, journal = Receiver(), Journal()
    try:
        normal = uninterrupted()
        paused = reference(receiver, journal)
        return {"uninterrupted": normal, "paused_reference": paused,
                "manifest": journal.load(),
                "note": "Reference backpressures fast-source offers; no target protocol is implemented."}
    finally:
        receiver.close()
        journal.close()


def boundary_case(boundary):
    receiver, journal = Receiver(), Journal()
    try:
        try:
            reference(receiver, journal, boundary)
        except BoundaryStop:
            pass
        images = receiver.image(), journal.image()
        visible_before = receiver.rows()
    finally:
        receiver.close()
        journal.close()
    receiver, journal = Receiver(image=images[0]), Journal(image=images[1])
    try:
        result = resumed(receiver, journal)
        return {"boundary": boundary, "visible_before_restart": visible_before,
                "recovered": result,
                "restart_method": "committed SQLite logical images; not a process/power-loss test"}
    finally:
        receiver.close()
        journal.close()


def evidence():
    observation = observe()
    paused = observation["paused_reference"]
    return {
        "normal_rows": observation["uninterrupted"]["rows"],
        "reference_rows": paused["rows"],
        "reference_final_state": paused["state"],
        "reference_manifest": list(observation["manifest"]),
        "blocked_offers": paused["blocked_offers"],
        "peak_ingress_bytes": paused["peak_ingress_bytes"],
        "boundaries": [
            {"boundary": b, "visible_before_restart": data["visible_before_restart"],
             "recovered_rows": data["recovered"]["rows"],
             "recovered_watermarks": data["recovered"]["state"]["watermarks"]}
            for b in BOUNDARIES for data in [boundary_case(b)]
        ],
        "restart_method": "committed SQLite logical images; not a process/power-loss test"
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["observe", "boundaries", "evidence"])
    args = parser.parse_args()
    data = (observe() if args.mode == "observe" else evidence() if args.mode == "evidence"
            else [boundary_case(b) for b in BOUNDARIES])
    print(json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
