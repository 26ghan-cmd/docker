//! Minimal Parquet writer: one row group, one uncompressed PLAIN data page per column.

use std::io::Write;

pub enum Col {
    /// INT32 annotated DATE.
    Date(Vec<i32>),
    I64(Vec<i64>),
    /// Required UTF8 strings: concatenated bytes + end offsets.
    Str(Vec<u8>, Vec<u32>),
    /// Optional UTF8 strings: bytes + end offsets for present values, presence per row.
    OptStr(Vec<u8>, Vec<u32>, Vec<bool>),
}

impl Col {
    fn rows(&self) -> usize {
        match self {
            Col::Date(v) => v.len(),
            Col::I64(v) => v.len(),
            Col::Str(_, o) => o.len(),
            Col::OptStr(_, _, p) => p.len(),
        }
    }
}

struct Tw {
    buf: Vec<u8>,
    last: Vec<i16>,
}

fn zz32(v: i32) -> u64 {
    ((v << 1) ^ (v >> 31)) as u32 as u64
}
fn zz64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

impl Tw {
    fn new() -> Self {
        Tw { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                self.buf.push(b);
                break;
            }
            self.buf.push(b | 0x80);
        }
    }
    fn fh(&mut self, id: i16, t: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.buf.push(((d as u8) << 4) | t);
        } else {
            self.buf.push(t);
            self.varint(zz32(id as i32));
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.fh(id, 5);
        self.varint(zz32(v));
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.fh(id, 6);
        self.varint(zz64(v));
    }
    fn binf(&mut self, id: i16, b: &[u8]) {
        self.fh(id, 8);
        self.varint(b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn sbeg(&mut self, id: i16) {
        self.fh(id, 12);
        self.last.push(0);
    }
    fn send(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn lbeg(&mut self, id: i16, et: u8, n: usize) {
        self.fh(id, 9);
        if n < 15 {
            self.buf.push(((n as u8) << 4) | et);
        } else {
            self.buf.push(0xF0 | et);
            self.varint(n as u64);
        }
    }
    fn elem_sbeg(&mut self) {
        self.last.push(0);
    }
}

fn plain_strings(out: &mut Vec<u8>, data: &[u8], ends: &[u32]) {
    let mut s = 0usize;
    for &e in ends {
        let e = e as usize;
        out.extend_from_slice(&((e - s) as u32).to_le_bytes());
        out.extend_from_slice(&data[s..e]);
        s = e;
    }
}

/// Write `cols` (name, column) as a Parquet file at `path`.
pub fn write_file(path: &std::path::Path, cols: &[(&str, Col)]) -> std::io::Result<()> {
    let nrows = cols.first().map_or(0, |c| c.1.rows());
    let mut file: Vec<u8> = Vec::new();
    file.extend_from_slice(b"PAR1");
    // (type, offset, size) per column
    let mut chunks: Vec<(i32, i64, i64)> = Vec::new();
    if nrows > 0 {
        for (_, col) in cols {
            let mut pd: Vec<u8> = Vec::new();
            let ptype;
            match col {
                Col::Date(v) => {
                    ptype = 1;
                    pd.reserve(v.len() * 4);
                    for x in v {
                        pd.extend_from_slice(&x.to_le_bytes());
                    }
                }
                Col::I64(v) => {
                    ptype = 2;
                    pd.reserve(v.len() * 8);
                    for x in v {
                        pd.extend_from_slice(&x.to_le_bytes());
                    }
                }
                Col::Str(d, e) => {
                    ptype = 6;
                    pd.reserve(d.len() + 4 * e.len());
                    plain_strings(&mut pd, d, e);
                }
                Col::OptStr(d, e, p) => {
                    ptype = 6;
                    // definition levels: RLE/bit-packed hybrid, bit width 1, bit-packed runs
                    let mut lv: Vec<u8> = Vec::new();
                    let mut i = 0usize;
                    const RUN_GROUPS: usize = 1 << 16;
                    while i < p.len() {
                        let n = (p.len() - i).min(RUN_GROUPS * 8);
                        let groups = n.div_ceil(8);
                        let mut h = ((groups as u64) << 1) | 1;
                        loop {
                            let b = (h & 0x7f) as u8;
                            h >>= 7;
                            if h == 0 {
                                lv.push(b);
                                break;
                            }
                            lv.push(b | 0x80);
                        }
                        for g in 0..groups {
                            let mut byte = 0u8;
                            for k in 0..8 {
                                let j = i + g * 8 + k;
                                if j < i + n && p[j] {
                                    byte |= 1 << k;
                                }
                            }
                            lv.push(byte);
                        }
                        i += n;
                    }
                    pd.extend_from_slice(&(lv.len() as u32).to_le_bytes());
                    pd.extend_from_slice(&lv);
                    plain_strings(&mut pd, d, e);
                }
            }
            let mut ph = Tw::new();
            ph.i32f(1, 0); // DATA_PAGE
            ph.i32f(2, pd.len() as i32);
            ph.i32f(3, pd.len() as i32);
            ph.sbeg(5);
            ph.i32f(1, nrows as i32);
            ph.i32f(2, 0); // PLAIN
            ph.i32f(3, 3); // RLE
            ph.i32f(4, 3); // RLE
            ph.send();
            ph.buf.push(0);
            let off = file.len() as i64;
            file.extend_from_slice(&ph.buf);
            file.extend_from_slice(&pd);
            chunks.push((ptype, off, (ph.buf.len() + pd.len()) as i64));
        }
    }
    // FileMetaData
    let mut t = Tw::new();
    t.i32f(1, 1);
    t.lbeg(2, 12, cols.len() + 1);
    t.elem_sbeg();
    t.binf(4, b"schema");
    t.i32f(5, cols.len() as i32);
    t.send();
    for (name, col) in cols {
        t.elem_sbeg();
        let (ty, rep, conv, logical) = match col {
            Col::Date(_) => (1, 0, 6, 6),
            Col::I64(_) => (2, 0, -1, -1),
            Col::Str(..) => (6, 0, 0, 1),
            Col::OptStr(..) => (6, 1, 0, 1),
        };
        t.i32f(1, ty);
        t.i32f(3, rep);
        t.binf(4, name.as_bytes());
        if conv >= 0 {
            t.i32f(6, conv);
            t.sbeg(10);
            t.sbeg(logical as i16);
            t.send();
            t.send();
        }
        t.send();
    }
    t.i64f(3, nrows as i64);
    if nrows > 0 {
        t.lbeg(4, 12, 1);
        t.elem_sbeg();
        t.lbeg(1, 12, cols.len());
        let mut total = 0i64;
        for ((name, _), &(ty, off, size)) in cols.iter().zip(&chunks) {
            total += size;
            t.elem_sbeg();
            t.i64f(2, off);
            t.sbeg(3);
            t.i32f(1, ty);
            t.lbeg(2, 5, 2);
            t.varint(zz32(0));
            t.varint(zz32(3));
            t.lbeg(3, 8, 1);
            t.varint(name.len() as u64);
            t.buf.extend_from_slice(name.as_bytes());
            t.i32f(4, 0);
            t.i64f(5, nrows as i64);
            t.i64f(6, size);
            t.i64f(7, size);
            t.i64f(9, off);
            t.send();
            t.send();
        }
        t.i64f(2, total);
        t.i64f(3, nrows as i64);
        t.send();
    } else {
        t.lbeg(4, 12, 0);
    }
    t.binf(6, b"shopping-sessions fast");
    t.buf.push(0);
    file.extend_from_slice(&t.buf);
    file.extend_from_slice(&(t.buf.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    let mut f = std::fs::File::create(path)?;
    f.write_all(&file)?;
    Ok(())
}
