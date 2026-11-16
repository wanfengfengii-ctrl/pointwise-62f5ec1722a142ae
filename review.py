import json
from pathlib import Path
import re
import sys
import unittest
import checks
from lab import evidence


def check_document(path):
    text = Path(path).read_text(encoding="utf-8")
    blocks = re.findall(r"```json\s*\n(.*?)\n```", text, re.S)
    candidates = [json.loads(block) for block in blocks]
    records = [value for value in candidates if isinstance(value, dict) and "baseline" in value]
    if len(records) != 1:
        raise ValueError("one JSON evidence object with baseline required")
    record = records[0]
    if record["baseline"] != evidence():
        raise ValueError("BASELINE_EVIDENCE_MISMATCH")
    for strategy in ("aligned_spill", "unaligned_channel"):
        prediction = record["predictions"][strategy]
        for field in ("cut", "replay", "batch", "memory", "release", "boundaries"):
            if not isinstance(prediction[field], str) or not prediction[field].strip():
                raise ValueError("missing prediction: " + strategy + "." + field)
    citations = re.findall(r"([a-z_]+\.py):(\d+)", text)
    required = {"sources.py", "joiner.py", "receiver.py", "checkpoint.py", "lab.py"}
    seen = set()
    for filename, line in citations:
        if filename in required:
            if not 1 <= int(line) <= len(Path(filename).read_text().splitlines()):
                raise ValueError("invalid citation")
            seen.add(filename)
    if seen != required:
        raise ValueError("missing business call-chain citations")
    return record


def main():
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromModule(checks))
    if not result.wasSuccessful():
        raise SystemExit(1)
    record = check_document(sys.argv[1])
    print(json.dumps({"checked_baseline": record["baseline"],
                      "design_predictions": record["predictions"],
                      "notice": "Evidence and citation checks are not protocol-correctness proof."},
                     ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
