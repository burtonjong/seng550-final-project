"""Measure the frozen files used as benchmark inputs."""

import hashlib
import json
def snapshot_file_metadata(raw: bytes, observations: int) -> dict:
    capture = json.loads(raw)
    sources = dict(capture["series"])
    if "subregional_demand" in capture:
        sources["subregional_demand"] = capture["subregional_demand"]
    return {
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
        "source_rows": sum(len(payload["response"]["data"]) for payload in sources.values()),
        "observations": observations,
        "captured_at_utc": capture.get("captured_at_utc"),
    }
