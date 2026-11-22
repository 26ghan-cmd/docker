"""Tiny strict Parquet reader for checking our own output (PLAIN, uncompressed, v1 data pages).
Returns rows as tuples of bytes with columns ordered by name, formatted like the expected CSVs."""
import datetime
import os
import struct


class T:
    def __init__(self, b, p=0):
        self.b, self.p = b, p

    def byte(self):
        v = self.b[self.p]; self.p += 1; return v

    def varint(self):
        s = r = 0
        while True:
            c = self.byte(); r |= (c & 0x7F) << s; s += 7
            if not c & 0x80:
                return r

    def zz(self):
        v = self.varint(); return (v >> 1) ^ -(v & 1)

    def val(self, t):
        if t in (1, 2): return t == 1
        if t == 3: return self.byte()
        if t in (4, 5, 6): return self.zz()
        if t == 7: v = struct.unpack_from('<d', self.b, self.p)[0]; self.p += 8; return v
        if t == 8: n = self.varint(); v = bytes(self.b[self.p:self.p + n]); self.p += n; return v
        if t in (9, 10):
            h = self.byte(); n = h >> 4; et = h & 15
            if n == 15: n = self.varint()
            return [self.val(et) for _ in range(n)]
        if t == 12: return self.struct()
        raise ValueError(f"thrift type {t}")

    def struct(self):
        d = {}; last = 0
        while True:
            h = self.byte()
            if h == 0: return d
            t = h & 15; dl = h >> 4
            fid = last + dl if dl else self.zz()
            d[fid] = self.val(t); last = fid


def read_file(path):
    b = open(path, 'rb').read()
    assert b[:4] == b'PAR1' and b[-4:] == b'PAR1', 'magic'
    n = struct.unpack('<I', b[-8:-4])[0]
    t = T(b, len(b) - 8 - n)
    md = t.struct()
    assert t.p == len(b) - 8, 'footer length'
    schema = md[2]
    cols = schema[1:]
    assert schema[0][5] == len(cols)
    names = [c[4].decode() for c in cols]
    nrows = md[3]
    data = {nm: [] for nm in names}
    for rg in md.get(4, []):
        assert len(rg[1]) == len(cols)
        rg_rows = rg[3]
        for cc, se, nm in zip(rg[1], cols, names):
            cm = cc[3]
            assert cm[3] == [nm.encode()] and cm[4] == 0 and cm[1] == se[1]
            p = cm[9]; end = p + cm[7]; got = 0; vals = []
            while p < end:
                th = T(b, p); ph = th.struct(); p = th.p
                assert ph[1] == 0 and ph[2] == ph[3]
                dh = ph[5]; nv = dh[1]; assert dh[2] == 0
                page = b[p:p + ph[3]]; p += ph[3]
                q = 0
                if se[3] == 1:
                    ln = struct.unpack_from('<I', page, 0)[0]; q = 4
                    rl = T(page[4:4 + ln]); defs = []
                    while len(defs) < nv:
                        h = rl.varint()
                        if h & 1:
                            for _ in range((h >> 1)):
                                byte = rl.byte(); defs += [(byte >> k) & 1 for k in range(8)]
                        else:
                            v = rl.byte(); defs += [v] * (h >> 1)
                    defs = defs[:nv]; q += ln
                else:
                    defs = [1] * nv
                for dfl in defs:
                    if not dfl: vals.append(None); continue
                    if se[1] == 1: vals.append(struct.unpack_from('<i', page, q)[0]); q += 4
                    elif se[1] == 2: vals.append(struct.unpack_from('<q', page, q)[0]); q += 8
                    elif se[1] == 6:
                        ln = struct.unpack_from('<I', page, q)[0]; q += 4
                        vals.append(bytes(page[q:q + ln])); q += ln
                assert q == len(page), 'page fully consumed'
                got += nv
            assert got == rg_rows == cm[5]
            data[nm] += vals
    for nm in names:
        assert len(data[nm]) == nrows
    rows = []
    order = sorted(range(len(names)), key=lambda i: names[i])
    for r in range(nrows):
        row = []
        for i in order:
            v = data[names[i]][r]; se = cols[i]
            if v is None: row.append(b'')
            elif se.get(6) == 6:
                assert se[10] == {6: {}}
                row.append((datetime.date(1970, 1, 1) + datetime.timedelta(days=v)).isoformat().encode())
            elif isinstance(v, int): row.append(str(v).encode())
            else: row.append(v)
        rows.append(tuple(row))
    return sorted(names), rows


def read_dir(d):
    rows = []; names = None
    for f in sorted(os.listdir(d)):
        if f.startswith(('.', '_')):
            continue
        p = os.path.join(d, f)
        if f.endswith('.parquet'):
            nm, r = read_file(p)
        elif f.endswith('.csv'):
            from readout import load_table
            r = load_table(p); nm = None
        else:
            continue
        if nm is not None:
            assert names is None or names == nm, (names, nm); names = nm
        rows += r
    return rows
