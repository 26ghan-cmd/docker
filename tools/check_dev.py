#!/usr/bin/env python3
"""check_dev.py OUT : compare OUT/{user_daily,campaign_daily} (parquet) with the published
dev answer (seed 1, 1M events) by SHA-256 of the sorted CSV rows."""
import hashlib, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from pq import read_dir
EXP = {'user_daily': ('d0f83bb599ce8918fe5e76d83eeecd0609c7f3aec667ad5b8e7ab3f0dc35032b', 118107,
                      ['user_id', 'day', 'sessions', 'events', 'revenue_cents', 'top_category']),
       'campaign_daily': ('25f28fbb67c112dcb7e3e543fbce6ed1f756dc04d867f99ca6ab5858d3f26200', 27449,
                          ['campaign_id', 'channel', 'day', 'sessions', 'purchasing_sessions', 'revenue_cents', 'assisted_sessions'])}
ok = True
for t, (h, n, order) in EXP.items():
    rows = read_dir(os.path.join(sys.argv[1], t)); names = sorted(order)
    lines = sorted(b','.join(r[names.index(c)] for c in order) for r in rows)
    got = hashlib.sha256(b'\n'.join(lines) + b'\n').hexdigest()
    good = got == h and len(rows) == n; ok &= good
    print(f"{t}: {len(rows)} rows {'MATCH' if good else 'DIFF'}")
sys.exit(0 if ok else 1)
