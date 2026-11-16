import json
import sqlite3


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class Receiver:
    """A separate transactional result service. Batch contents are immutable."""
    def __init__(self, path=":memory:", image=None):
        self.db = sqlite3.connect(path)
        if image is not None:
            self.db.executescript(image)
        self.db.execute("CREATE TABLE IF NOT EXISTS batches(id TEXT PRIMARY KEY, body TEXT, visible INTEGER)")
        self.db.commit()

    def stage(self, batch, rows):
        body = canonical(sorted(rows))
        old = self.db.execute("SELECT body FROM batches WHERE id=?", (batch,)).fetchone()
        if old and old[0] != body:
            raise ValueError("BATCH_CONTENT_CONFLICT")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO batches VALUES(?,?,0)", (batch, body))

    def commit(self, batch, lose_response=False):
        with self.db:
            changed = self.db.execute("UPDATE batches SET visible=1 WHERE id=?", (batch,)).rowcount
            if not changed:
                raise KeyError(batch)
        if lose_response:
            raise ConnectionError("ACK_LOST_AFTER_COMMIT")

    def rows(self):
        rows = []
        for body, in self.db.execute("SELECT body FROM batches WHERE visible=1"):
            rows.extend(json.loads(body))
        return sorted(rows)

    def image(self):
        return "\n".join(self.db.iterdump())

    def close(self):
        self.db.close()
