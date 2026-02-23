import os, sys, json, glob, sqlite3
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
CANDIDATE_GLOBS = [
    str(ROOT / "data" / "*.mbtiles"),
    str(ROOT / "tiles" / "*.mbtiles"),
    str(ROOT / "*.mbtiles"),
]
OUT_DIR = ROOT / "osm_pulled_data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def read_meta_json(conn, key):
    cur = conn.execute("SELECT value FROM metadata WHERE name=?", (key,))
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None

def read_meta_text(conn, key):
    cur = conn.execute("SELECT value FROM metadata WHERE name=?", (key,))
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else None

def inspect_one(path: Path):
    item = {"file": path.name}
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except Exception as e:
        item["error"] = str(e)
        return item

    try:
        # try common places vector layer info can live
        vls = None

        style_json = read_meta_json(conn, "json")
        if isinstance(style_json, dict) and "vector_layers" in style_json:
            vls = style_json["vector_layers"]

        if vls is None:
            vls = read_meta_json(conn, "vector_layers")

        if vls is None:
            tilestats = read_meta_json(conn, "tilestats")
            if isinstance(tilestats, dict) and "layers" in tilestats:
                vls = []
                for L in tilestats["layers"]:
                    lid = L.get("layer")
                    attrs = [a.get("attribute") for a in (L.get("attributes") or []) if a.get("attribute")]
                    vls.append({"id": lid, "fields": {a: "?" for a in attrs}})

        layers = []
        for L in (vls or []):
            lid = L.get("id") or L.get("layer")
            fields_dict = L.get("fields") or {}
            fields = sorted(list(fields_dict.keys()))
            if lid:
                layers.append({"id": lid, "fields": fields})

        item["vector_layers"] = layers

        for k in ("minzoom","maxzoom","bounds","center"):
            val = read_meta_json(conn, k)
            if val is None:
                txt = read_meta_text(conn, k)
                if txt is not None:
                    item[k] = txt
            else:
                item[k] = val
    except Exception as e:
        item = {"file": path.name, "error": str(e)}
    finally:
        try: conn.close()
        except: pass
    return item

def main():
    patterns = sys.argv[1:] or CANDIDATE_GLOBS
    paths = []
    for pat in patterns:
        paths.extend(glob.glob(pat))
    paths = [Path(p) for p in sorted(set(paths)) if p.lower().endswith(".mbtiles")]

    if not paths:
        print("No .mbtiles found. Put them in ./data or ./tiles, or pass paths as arguments.")
        sys.exit(1)

    items = [inspect_one(p) for p in paths]

    # union summary
    layers_seen = {}
    fields_union = {}
    for it in items:
        for L in it.get("vector_layers") or []:
            lid = L["id"]
            layers_seen.setdefault(lid, set()).add(it["file"])
            fields_union.setdefault(lid, set()).update(L.get("fields") or [])

    schema = {"generatedAt": datetime.utcnow().isoformat()+"Z", "items": items}
    (OUT_DIR / "mbtiles_schema.json").write_text(json.dumps(schema, indent=2))

    md = []
    md.append("# MBTiles Data Summary")
    md.append(f"\nFiles: {len(items)}")
    md.append("\n## Layers present\n")
    for lid in sorted(layers_seen):
        md.append(f"- {lid} ({len(layers_seen[lid])} files)")
    md.append("\n## Fields by layer\n")
    for lid in sorted(fields_union):
        fields = ", ".join(sorted(fields_union[lid])) or "(none)"
        md.append(f"- **{lid}**: {fields}")
    (OUT_DIR / "report_from_mbtiles.md").write_text("\n".join(md))

    print(f"Wrote {OUT_DIR/'mbtiles_schema.json'}")
    print(f"Wrote {OUT_DIR/'report_from_mbtiles.md'}")

if __name__ == "__main__":
    main()
