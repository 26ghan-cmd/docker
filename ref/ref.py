#!/usr/bin/env python3
"""Reference implementation of rules.md. Slow but literal. Writes CSV tables.

usage: ref.py DATA_DIR OUT_DIR   -> OUT_DIR/user_daily.csv, OUT_DIR/campaign_daily.csv
"""
import glob
import json
import os
import sys
from collections import defaultdict

DAY = 86_400_000
GAP = 1_800_000
WEEK = 7 * DAY
DIG = set(b"0123456789")


def lines_of(path):
    data = open(path, "rb").read()
    parts = data.split(b"\n")
    if parts and parts[-1] == b"":
        parts.pop()
    return parts[1:]  # skip header


def parse_line(line, n):
    fields = line.split(b",")
    if len(fields) != n:
        return None
    out = []
    for f in fields:
        if b'"' in f:
            if len(f) < 2 or f[0] != 0x22 or f[-1] != 0x22 or b'"' in f[1:-1]:
                return None
            f = f[1:-1]
        out.append(f if f != b"" else None)
    return out


def is_digits(b):
    return len(b) > 0 and all(c in DIG for c in b)


def days_from_civil(y, m, d):
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def leap(y):
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


MDAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def parse_ts(b):
    """Return epoch ms (int, possibly huge) or None if bad."""
    if b is None:
        return None
    if is_digits(b):
        return int(b)
    L = len(b)
    if L not in (20, 25):
        return None
    if not (b[4] == 0x2D and b[7] == 0x2D and b[10] == 0x54 and b[13] == 0x3A and b[16] == 0x3A):
        return None
    for i in (0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18):
        if b[i] not in DIG:
            return None
    y = int(b[0:4]); mo = int(b[5:7]); d = int(b[8:10])
    h = int(b[11:13]); mi = int(b[14:16]); s = int(b[17:19])
    if not (1 <= mo <= 12):
        return None
    md = 29 if (mo == 2 and leap(y)) else MDAYS[mo - 1]
    if not (1 <= d <= md) or h > 23 or mi > 59 or s > 59:
        return None
    off = 0
    if L == 20:
        if b[19] != 0x5A:
            return None
    else:
        sg = b[19]
        if sg not in (0x2B, 0x2D) or b[22] != 0x3A:
            return None
        for i in (20, 21, 23, 24):
            if b[i] not in DIG:
                return None
        oh = int(b[20:22]); om = int(b[23:25])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        if sg == 0x2D:
            off = -off
    ms = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000
    return ms - off


def main(data_dir, out_dir):
    man = json.load(open(os.path.join(data_dir, "manifest.json")))
    ws = parse_ts(man["window_start"].encode())
    we = parse_ts(man["window_end"].encode())

    products = {}
    for ln in lines_of(os.path.join(data_dir, "products.csv")):
        r = parse_line(ln, 2)
        if r and r[0] is not None:
            products[r[0]] = r[1]
    campaigns = {}
    for ln in lines_of(os.path.join(data_dir, "campaigns.csv")):
        r = parse_line(ln, 2)
        if r and r[0] is not None:
            campaigns[r[0]] = r[1]

    # ---- links: union-find
    parent = {}

    def find(x):
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        while parent.get(x, x) != root:
            nxt = parent[x]
            parent[x] = root
            x = nxt
        return root

    for p in sorted(glob.glob(os.path.join(data_dir, "links", "*.csv"))):
        for ln in lines_of(p):
            r = parse_line(ln, 2)
            if r is None or r[0] is None or r[1] is None:
                continue
            a, b = find(r[0]), find(r[1])
            if a != b:
                if b < a:
                    a, b = b, a
                parent[b] = a  # root is always the smallest id of the component
            else:
                parent.setdefault(a, a)

    def person(u):
        return find(u) if u in parent else u

    # ---- events
    best = {}  # event_id -> (ingest_bytes, fields)
    for p in sorted(glob.glob(os.path.join(data_dir, "events", "*.csv"))):
        for ln in lines_of(p):
            r = parse_line(ln, 7)
            if r is None or r[0] is None:
                continue
            ing = r[3] if r[3] is not None else b""
            cur = best.get(r[0])
            if cur is None or parse_ts(ing) < parse_ts(cur[0]):
                best[r[0]] = (ing, r)
    by_person = defaultdict(list)
    for eid, (ing, r) in best.items():
        _, uid, ts_b, ing_b, et, pid, price_b = r
        if uid is None or pid is None:
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (ws <= ts < we):
            continue
        if et not in (b"view", b"cart", b"purchase"):
            continue
        price = 0
        if et == b"purchase":
            if price_b is None or not is_digits(price_b):
                continue
            price = int(price_b)
            if not (1 <= price <= 10_000_000):
                continue
        ingms = parse_ts(ing_b)
        if ingms - ts > DAY:
            continue
        cat = products.get(pid, b"unknown")
        by_person[person(uid)].append((ts, et == b"purchase", price, cat))

    # ---- clicks
    clicks = {}
    for p in sorted(glob.glob(os.path.join(data_dir, "clicks", "*.csv"))):
        for ln in lines_of(p):
            r = parse_line(ln, 4)
            if r is None or r[0] is None:
                continue
            clicks.setdefault(r[0], r)
    clicks_by_person = defaultdict(list)
    for cid, (_, uid, camp, ts_b) in clicks.items():
        if uid is None:
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (ws - WEEK <= ts < we):
            continue
        if camp not in campaigns:
            continue
        clicks_by_person[person(uid)].append((ts, cid, camp))

    ud = defaultdict(lambda: [0, 0, 0, defaultdict(int)])
    cd = defaultdict(lambda: [0, 0, 0, 0])
    for per, evs in by_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        prev = None
        for e in evs:
            if prev is None or e[0] - prev > GAP:
                sessions.append([])
            sessions[-1].append(e)
            prev = e[0]
        pcl = clicks_by_person.get(per, [])
        for s in sessions:
            start = s[0][0]
            day = start // DAY
            rev = sum(e[2] for e in s)
            has_p = any(e[1] for e in s)
            u = ud[(per, day)]
            u[0] += 1
            u[1] += len(s)
            u[2] += rev
            for e in s:
                if e[1]:
                    u[3][e[3]] += e[2]
            q = [c for c in pcl if start - WEEK <= c[0] <= start]
            if q:
                # latest ts, then smallest click_id
                win = sorted(q, key=lambda c: (-c[0], c[1]))[0]
                camp = win[2]
                for cp in {c[2] for c in q}:
                    cd[(cp, day)][3] += 1
            else:
                camp = b"organic"
            c = cd[(camp, day)]
            c[0] += 1
            c[1] += 1 if has_p else 0
            c[2] += rev

    os.makedirs(out_dir, exist_ok=True)
    import datetime
    def dstr(d):
        return (datetime.date(1970, 1, 1) + datetime.timedelta(days=d)).isoformat().encode()

    with open(os.path.join(out_dir, "user_daily.csv"), "wb") as f:
        f.write(b"user_id,day,sessions,events,revenue_cents,top_category\n")
        for (per, day), (s, n, rev, cats) in sorted(ud.items()):
            top = b""
            if cats:
                top = sorted(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
            f.write(b",".join([per, dstr(day), str(s).encode(), str(n).encode(), str(rev).encode(), top]) + b"\n")
    with open(os.path.join(out_dir, "campaign_daily.csv"), "wb") as f:
        f.write(b"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n")
        for (camp, day), (s, ps, rev, a) in sorted(cd.items()):
            ch = b"organic" if camp == b"organic" else campaigns[camp]
            f.write(b",".join([camp, ch, dstr(day), str(s).encode(), str(ps).encode(), str(rev).encode(), str(a).encode()]) + b"\n")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
