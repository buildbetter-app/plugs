#!/usr/bin/env python3
"""Project a recorded source outcome without changing array order or strings."""

import hashlib
import json


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def project(raw):
    if not isinstance(raw, dict) or raw.get("card") not in {"B09", "B10", "B11", "B12"}:
        raise ValueError("recording must name one assigned card")
    outcomes = raw.get("outcomes")
    if not isinstance(outcomes, list):
        raise ValueError("recording outcomes must be an ordered list")
    projection = {
        "card": raw["card"],
        "outcomes": outcomes,
        "decisionIDs": raw.get("decisionIDs", []),
        "disposition": raw.get("disposition", ""),
    }
    data = canonical(projection).encode("utf-8")
    return {"json": data.decode("utf-8"), "sha256": hashlib.sha256(data).hexdigest()}
