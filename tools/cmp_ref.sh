#!/bin/sh
# cmp_ref.sh SEED EVENTS CLICKS : generate, run ref + fast, compare
set -e
d=/tmp/seed$1; rm -rf $d $d.r $d.f
${GENERATE:-generate} --seed $1 --events $2 --clicks $3 --output $d >/dev/null
python3 ref/ref.py $d $d.r
fast/target/release/ss-fast $d $d.f
python3 - $d <<'PY'
import sys; sys.path.insert(0,"tools")
from pq import read_dir
from readout import load_table
d=sys.argv[1]; ok=True
for t in ['user_daily','campaign_daily']:
    a=load_table(d+'.r/'+t+'.csv'); b=sorted(read_dir(d+'.f/'+t))
    if a!=b:
        ok=False; sa,sb=set(a),set(b); print(t,'DIFF', len(a),len(b),'only ref',list(sa-sb)[:3],'only fast',list(sb-sa)[:3])
print(d, 'OK' if ok else 'MISMATCH')
PY
rm -rf $d $d.r $d.f
