#!/usr/bin/env python3
import sys, os, json, shutil
def fix_array_json(path):
    bak = path + ".bak"
    shutil.copy2(path, bak)
    with open(path, "r", encoding="utf-8") as f:
        s = f.read().strip()
    if not s:
        with open(path, "w", encoding="utf-8") as f:
            f.write("[]")
        print("reset []"); return
    try:
        json.loads(s); print("ok"); return
    except Exception:
        pass
    if not s.startswith("["):
        print("not-array"); return
    last_good = None
    for i in range(len(s)-1, 0, -1):
        if s[i] == "}":
            cand = s[:i+1].rstrip(", \t\r\n") + "]"
            try:
                json.loads(cand); last_good = cand; break
            except Exception:
                continue
    if last_good is None:
        for cut in range(len(s)-1, max(len(s)-5000, 1), -1):
            cand = s[:cut].rstrip(", \t\r\n") + "]"
            try:
                json.loads(cand); last_good = cand; break
            except Exception:
                pass
    if last_good is None:
        print("fail"); return
    with open(path, "w", encoding="utf-8") as f:
        f.write(last_good)
    print("fixed")
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python3 tools/repair_json_array.py <json_path>"); sys.exit(1)
    fix_array_json(sys.argv[1])
