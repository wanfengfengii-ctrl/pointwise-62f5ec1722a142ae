import copy


class Joiner:
    """All pairs within distance 2; strict opposite-watermark finalization."""
    def __init__(self, state=None):
        self.state = copy.deepcopy(state) if state else {
            "watermarks": {"L": -10**18, "R": -10**18},
            "active": {"L": {}, "R": {}}
        }

    def accept(self, side, event):
        other = "R" if side == "L" else "L"
        active, watermarks = self.state["active"], self.state["watermarks"]
        out = []
        if event["kind"] == "row":
            if event["t"] < watermarks[side]:
                raise ValueError("late source row")
            row = {"t": event["t"], "matched": False}
            active[side][event["id"]] = row
            for oid, candidate in active[other].items():
                if abs(row["t"] - candidate["t"]) <= 2:
                    row["matched"] = candidate["matched"] = True
                    lid, rid = (event["id"], oid) if side == "L" else (oid, event["id"])
                    out.append(["pair", lid, rid])
        elif event["kind"] == "wm":
            if event["t"] < watermarks[side]:
                raise ValueError("watermark regression")
            watermarks[side] = event["t"]
        else:
            raise ValueError("operator accepts rows and watermarks only")
        for s in ("L", "R"):
            opposite = "R" if s == "L" else "L"
            expired = [i for i, r in active[s].items()
                       if watermarks[opposite] > r["t"] + 2]
            for rid in expired:
                row = active[s].pop(rid)
                if not row["matched"]:
                    out.append(["unmatched", s, rid])
        return out

    def snapshot(self):
        return copy.deepcopy(self.state)
