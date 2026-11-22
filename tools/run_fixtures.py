#!/usr/bin/env python3
"""run_fixtures.py FIXTURES -- CMD...   CMD gets DATA OUT; OUT may hold csv files or parquet dirs."""
import os, subprocess, sys, tempfile, shutil
sys.path.insert(0, os.path.dirname(__file__))
from readout import load_table
fx = sys.argv[1]; cmd = sys.argv[sys.argv.index('--')+1:]
bad = 0; cases = sorted(os.listdir(fx))
cases = [c for c in cases if os.path.isdir(os.path.join(fx, c))]
for c in cases:
    out = tempfile.mkdtemp()
    r = subprocess.run(cmd + [os.path.join(fx, c, 'input'), out], capture_output=True)
    ok = r.returncode == 0
    msg = r.stderr.decode(errors='replace')[-500:]
    if ok:
        for t in ('user_daily', 'campaign_daily'):
            exp = load_table(os.path.join(fx, c, 'expected', t + '.csv'))
            got = load_table(os.path.join(out, t))
            if exp != got:
                ok = False; msg += f"\n {t}\n  expected {exp}\n  got      {got}"
    print(('PASS ' if ok else 'FAIL ') + c + ('' if ok else msg))
    bad += not ok
    shutil.rmtree(out)
print(f"{len(cases)-bad}/{len(cases)} passed")
sys.exit(1 if bad else 0)
