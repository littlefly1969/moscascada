#!/usr/bin/env python3
"""VS-005: validate an alarm catalog (docs/catalog/alarm-catalog.schema.json) against a kernel
configuration, with the standard library only.

  check_alarm_catalog.py CATALOG.json CONFIG.toml

Checks the structural rules of the schema that matter for referential integrity (required
fields, enums, id patterns, no unknown keys in entries) and the cross references: every
alarm_id exists in [[alarms.rules]], every related tag exists in [[tags]], every
asset/document/graphic reference is declared (1:N and N:M relations), alarm ids unique.
Exit 1 with one line per problem. The kernel never reads the catalog.
"""
import json
import re
import sys
import tomllib

ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
LANG = re.compile(r"^[a-z]{2}(-[A-Z]{2})?$")
ENTRY_KEYS = {"alarm_id", "title", "description", "operator_instructions", "causes", "suggestions",
              "related_tag_ids", "asset_ids", "document_ids", "graphics"}


def check(cat, cfg):
    errs = []
    e = errs.append
    if cat.get("catalog_schema_version") != 1:
        e("catalog_schema_version must be 1")
    for k in ("catalog_version", "config_fingerprint", "languages", "alarms"):
        if k not in cat:
            e(f"missing {k}")
    langs = set(cat.get("languages", []))
    if not langs or not all(LANG.match(x) for x in langs):
        e("languages: non-empty list of language codes")

    def text(where, t):
        if not isinstance(t, dict) or not t:
            e(f"{where}: multilingual text object required")
            return
        for lang in t:
            if lang not in langs:
                e(f"{where}: language {lang!r} not declared in languages")

    def ids(kind, key):
        out = set()
        for x in cat.get(kind, []):
            i = x.get(key)
            if not isinstance(i, str) or not ID.match(i):
                e(f"{kind}: invalid {key} {i!r}")
            elif i in out:
                e(f"{kind}: duplicate {key} {i!r}")
            out.add(i)
            text(f"{kind}[{i}].title", x.get("title"))
        return out

    assets, graphics, docs = ids("assets", "asset_id"), ids("graphics", "graphic_id"), ids("documents", "document_id")
    for a in cat.get("assets", []):
        if "parent_asset_id" in a and a["parent_asset_id"] not in assets:
            e(f"asset {a.get('asset_id')}: unknown parent {a['parent_asset_id']!r}")
    rules = {r["id"] for r in cfg.get("alarms", {}).get("rules", [])}
    tags = {t["id"] for t in cfg.get("tags", [])}
    seen = set()
    for entry in cat.get("alarms", []):
        aid = entry.get("alarm_id")
        w = f"alarm {aid}"
        if set(entry) - ENTRY_KEYS:
            e(f"{w}: unknown keys {sorted(set(entry) - ENTRY_KEYS)} (severity/thresholds belong to the kernel config)")
        if not isinstance(aid, int) or not 1 <= aid <= 0xFFFFFFFF:
            e(f"{w}: alarm_id must be a u32 != 0")
        elif aid in seen:
            e(f"{w}: duplicate alarm_id")
        elif aid not in rules:
            e(f"{w}: no [[alarms.rules]] with this id in the configuration")
        seen.add(aid)
        text(f"{w}.title", entry.get("title"))
        for k in ("description", "operator_instructions"):
            if k in entry:
                text(f"{w}.{k}", entry[k])
        for k in ("causes", "suggestions"):
            for i, t in enumerate(entry.get(k, [])):
                text(f"{w}.{k}[{i}]", t)
        for t in entry.get("related_tag_ids", []):
            if t not in tags:
                e(f"{w}: related tag {t} not in [[tags]]")
        for a in entry.get("asset_ids", []):
            if a not in assets:
                e(f"{w}: unknown asset {a!r}")
        for d in entry.get("document_ids", []):
            if d not in docs:
                e(f"{w}: unknown document {d!r}")
        for g in entry.get("graphics", []):
            if set(g) - {"graphic_id", "element_id", "action"}:
                e(f"{w}: graphic ref with unknown keys")
            if g.get("graphic_id") not in graphics:
                e(f"{w}: unknown graphic {g.get('graphic_id')!r}")
            if g.get("action") not in ("Open", "OpenAndHighlight"):
                e(f"{w}: action must be Open | OpenAndHighlight")
            if "element_id" in g and not ID.match(str(g["element_id"])):
                e(f"{w}: invalid element_id")
    return errs


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    with open(sys.argv[1]) as f:
        cat = json.load(f)
    with open(sys.argv[2], "rb") as f:
        cfg = tomllib.load(f)
    errs = check(cat, cfg)
    for x in errs:
        print(f"catalog: {x}")
    if errs:
        sys.exit(1)
    print(f"catalog ok: {len(cat['alarms'])} entries, version {cat['catalog_version']}")


if __name__ == "__main__":
    main()
