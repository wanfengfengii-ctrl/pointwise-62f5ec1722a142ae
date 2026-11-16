import json
import sqlite3
from receiver import canonical
from joiner import Joiner
from sources import Sources


class BoundaryStop(Exception):
    pass


class Journal:
    """The reference manifest stores a quiescent cut, not channel state."""
    def __init__(self, path=":memory:", image=None):
        self.db = sqlite3.connect(path)
        if image is not None:
            self.db.executescript(image)
        self.db.execute("CREATE TABLE IF NOT EXISTS checkpoint(id TEXT PRIMARY KEY, body TEXT, complete INTEGER)")
        self.db.commit()

    def prepare(self, payload):
        body = canonical(payload)
        old = self.db.execute("SELECT body FROM checkpoint WHERE id='c1'").fetchone()
        if old and old[0] != body:
            raise ValueError("CHECKPOINT_CONTENT_CONFLICT")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO checkpoint VALUES('c1',?,0)", (body,))

    def complete(self):
        with self.db:
            self.db.execute("UPDATE checkpoint SET complete=1 WHERE id='c1'")

    def load(self):
        row = self.db.execute("SELECT body,complete FROM checkpoint WHERE id='c1'").fetchone()
        return (json.loads(row[0]), bool(row[1])) if row else None

    def image(self):
        return "\n".join(self.db.iterdump())

    def close(self):
        self.db.close()


def stop_at(requested, boundary):
    if requested == boundary:
        raise BoundaryStop(boundary)


def quiescent(sources, operator, pending, receiver, journal, fault=None):
    if sources.positions != sources.barrier_positions():
        raise ValueError("REFERENCE_REQUIRES_BOTH_BARRIERS")
    payload = {"positions": dict(sources.positions),
               "state": operator.snapshot(), "pending": pending}
    stop_at(fault, "before_prepare")
    journal.prepare(payload)
    stop_at(fault, "after_prepare")
    receiver.stage("c1", payload["pending"])
    stop_at(fault, "after_stage")
    receiver.commit("c1")
    stop_at(fault, "after_sink_commit")
    journal.complete()
    stop_at(fault, "after_complete")
    return []


def restore(logs, receiver, journal):
    saved = journal.load()
    if saved is None:
        return Sources(logs), Joiner(), []
    payload, complete = saved
    if not complete:
        receiver.stage("c1", payload["pending"])
        receiver.commit("c1")
        journal.complete()
    return Sources(logs, payload["positions"]), Joiner(payload["state"]), []
