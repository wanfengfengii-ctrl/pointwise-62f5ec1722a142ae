def row(identity, time):
    return {"kind": "row", "id": identity, "t": time}


def wm(time):
    return {"kind": "wm", "t": time}


BARRIER = {"kind": "barrier", "checkpoint": "c1"}
LOGS = {
    "L": [row("l0", 0), row("lwait", 10), BARRIER,
          row("l2", 20), row("l3", 24), row("l4", 28),
          row("l5", 32), row("l6", 36), row("l7", 40), wm(100)],
    "R": [row("r0", 1), wm(12), row("r_edge", 12),
          row("r1", 20), wm(23), BARRIER, row("r4", 28), wm(100)]
}
# Every token is an offer to receive the next event on that source.
# Source order is fixed; the reference may backpressure an offer.
SCHEDULE = ["L", "R", "L", "L"] + ["L"] * 6 + ["R"] * 5 + ["L"] + ["R"] * 2
EXPECTED = sorted([
    ["pair", "l0", "r0"], ["pair", "lwait", "r_edge"],
    ["pair", "l2", "r1"], ["pair", "l4", "r4"],
    ["unmatched", "L", "l3"], ["unmatched", "L", "l5"],
    ["unmatched", "L", "l6"], ["unmatched", "L", "l7"]
])
