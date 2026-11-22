//! Shopping Sessions: fast native pipeline (Rust std only).
//!
//! Reads $DATA_DIR (default /data) and writes $OUTPUT_DIR (default /output)/{user_daily,campaign_daily}/*.parquet.
//! For local testing: `ss-fast DATA OUT [--csv]`. Set SS_TIMING=1 for phase timings.
//!
//! Pipeline (all phases parallel unless noted):
//!   A  parse every CSV chunk: events validated and bucketed by event_id hash; clicks validated; link lines.
//!   L  union-find over linked IDs (serial); each person named by its byte-wise smallest ID.
//!   B  dedup events per bucket (earliest ingest_ts wins, before validation), map users and clicks to people,
//!      bucket by person.
//!   C  per person bucket: sort, sessionize, credit and assists, aggregate.
//!   W  write Parquet.

mod parquet;

use parquet::Col;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const BAD: i64 = i64::MIN;
/// Buckets for event-id dedup and for people.
const PBITS: u32 = 9;
const P: usize = 1 << PBITS;

type S = &'static [u8];

// ---------------------------------------------------------------- allocator

/// System allocator that asks for transparent huge pages on large blocks (fewer TLB misses
/// in the big hash tables and buckets). Harmless where THP is off.
struct HugeAlloc;

const HUGE_MIN: usize = 4 << 20;

#[inline]
fn advise_huge(p: *mut u8, size: usize) {
    if size >= HUGE_MIN && !p.is_null() {
        let a = (p as usize + (2 << 20) - 1) & !((2 << 20) - 1);
        let end = (p as usize + size) & !((2 << 20) - 1);
        if end > a {
            unsafe { sys::madvise(a as *mut _, end - a, 14) }; // MADV_HUGEPAGE
        }
    }
}

unsafe impl std::alloc::GlobalAlloc for HugeAlloc {
    unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
        let p = unsafe { std::alloc::System.alloc(l) };
        advise_huge(p, l.size());
        p
    }
    unsafe fn alloc_zeroed(&self, l: std::alloc::Layout) -> *mut u8 {
        let p = unsafe { std::alloc::System.alloc_zeroed(l) };
        advise_huge(p, l.size());
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: std::alloc::Layout, n: usize) -> *mut u8 {
        let q = unsafe { std::alloc::System.realloc(p, l, n) };
        advise_huge(q, n);
        q
    }
}

#[global_allocator]
static GLOBAL: HugeAlloc = HugeAlloc;

// ---------------------------------------------------------------- files

mod sys {
    use std::os::raw::{c_int, c_long, c_void};
    unsafe extern "C" {
        pub fn mmap(addr: *mut c_void, len: usize, prot: c_int, flags: c_int, fd: c_int, off: c_long) -> *mut c_void;
        pub fn madvise(addr: *mut c_void, len: usize, advice: c_int) -> c_int;
    }
}

fn map_file(path: &Path) -> S {
    use std::os::unix::io::AsRawFd;
    let Ok(f) = std::fs::File::open(path) else {
        return &[];
    };
    let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    unsafe {
        let p = sys::mmap(std::ptr::null_mut(), len, 1, 2, f.as_raw_fd(), 0);
        if p as isize == -1 {
            let v = std::fs::read(path).unwrap_or_default();
            return Box::leak(v.into_boxed_slice());
        }
        sys::madvise(p, len, 3); // MADV_WILLNEED
        std::slice::from_raw_parts(p as *const u8, len)
    }
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| {
                let n = e.file_name();
                let n = n.to_string_lossy();
                n.ends_with(".csv") && !n.starts_with('.') && e.path().is_file()
            })
            .map(|e| e.path())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

#[inline(always)]
fn load8(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

/// High bit set in every byte of `x` equal to `c` (exact).
#[inline(always)]
fn eqmask(x: u64, c: u8) -> u64 {
    let y = x ^ (0x0101_0101_0101_0101u64.wrapping_mul(c as u64));
    !(((y & 0x7f7f_7f7f_7f7f_7f7f).wrapping_add(0x7f7f_7f7f_7f7f_7f7f)) | y | 0x7f7f_7f7f_7f7f_7f7f)
}

#[inline]
fn find_nl(b: &[u8], from: usize) -> usize {
    let n = b.len();
    let mut i = from;
    while i + 8 <= n {
        let m = eqmask(load8(b, i), b'\n');
        if m != 0 {
            return i + (m.trailing_zeros() / 8) as usize;
        }
        i += 8;
    }
    while i < n {
        if b[i] == b'\n' {
            return i;
        }
        i += 1;
    }
    n
}

/// Chunks of a file aligned to line starts: (bytes, skip_header).
fn chunks_of(data: S, target: usize, out: &mut Vec<(S, bool)>) {
    let mut start = 0;
    let len = data.len();
    while start < len {
        let mut end = (start + target).min(len);
        if end < len {
            end = (find_nl(data, end) + 1).min(len);
        }
        out.push((&data[start..end], start == 0));
        start = end;
    }
}

/// Bitmasks of newline, comma and quote bytes in a 64-byte block.
#[inline(always)]
fn block_masks(p: *const u8) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use core::arch::x86_64::*;
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let n = _mm256_set1_epi8(b'\n' as i8);
        let c = _mm256_set1_epi8(b',' as i8);
        let q = _mm256_set1_epi8(b'"' as i8);
        let m = |x: __m256i, y: __m256i| -> u64 {
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, y)) as u32 as u64
        };
        let nl = m(a, n) | (m(b, n) << 32);
        let cm = m(a, c) | (m(b, c) << 32);
        let qt = m(a, q) | (m(b, q) << 32);
        return (nl, cm, qt);
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    unsafe {
        use core::arch::x86_64::*;
        let n = _mm_set1_epi8(b'\n' as i8);
        let c = _mm_set1_epi8(b',' as i8);
        let q = _mm_set1_epi8(b'"' as i8);
        let (mut nl, mut cm, mut qt) = (0u64, 0u64, 0u64);
        for k in 0..4 {
            let x = _mm_loadu_si128(p.add(16 * k) as *const __m128i);
            nl |= (_mm_movemask_epi8(_mm_cmpeq_epi8(x, n)) as u32 as u64) << (16 * k);
            cm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(x, c)) as u32 as u64) << (16 * k);
            qt |= (_mm_movemask_epi8(_mm_cmpeq_epi8(x, q)) as u32 as u64) << (16 * k);
        }
        return (nl, cm, qt);
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let (mut nl, mut cm, mut qt) = (0u64, 0u64, 0u64);
        for k in 0..64 {
            let b = unsafe { *p.add(k) };
            nl |= ((b == b'\n') as u64) << k;
            cm |= ((b == b',') as u64) << k;
            qt |= ((b == b'"') as u64) << k;
        }
        (nl, cm, qt)
    }
}

/// Calls `f` with the fields of every well-formed line of `chunk` that has exactly N fields,
/// following the rules' CSV dialect: only `\n` ends a line, every comma separates fields,
/// quotes are allowed only around a whole field. Empty fields (quoted or not) are empty slices.
#[inline(always)]
fn for_records<const N: usize>(chunk: S, skip_header: bool, mut f: impl FnMut(&[S; N])) {
    let n = chunk.len();
    let p0 = if skip_header { (find_nl(chunk, 0) + 1).min(n) } else { 0 };
    if p0 >= n {
        return;
    }
    let mut ends = [0usize; N];
    let mut nc = 0usize; // separators seen in the current line
    let mut quotes = 0u32;
    let mut line_start = p0;
    let mut emit = |line_start: usize, line_end: usize, nc: usize, quotes: u32, ends: &mut [usize; N]| {
        if nc != N - 1 {
            return;
        }
        ends[N - 1] = line_end;
        let mut fl: [S; N] = [&[]; N];
        let mut s = line_start;
        for k in 0..N {
            fl[k] = unsafe { chunk.get_unchecked(s..ends[k]) };
            s = ends[k] + 1;
        }
        if quotes > 0 {
            let mut wrapped = 0u32;
            for v in fl.iter_mut() {
                let l = v.len();
                if l >= 2 && v[0] == b'"' && v[l - 1] == b'"' {
                    wrapped += 1;
                    *v = &v[1..l - 1];
                }
            }
            if quotes != 2 * wrapped {
                return;
            }
        }
        f(&fl);
    };
    let mut base = p0;
    while base < n {
        let avail = n - base;
        let (mut nl, mut cm, mut qt) = if avail >= 64 {
            block_masks(unsafe { chunk.as_ptr().add(base) })
        } else {
            let mut pad = [0u8; 64];
            pad[..avail].copy_from_slice(&chunk[base..]);
            block_masks(pad.as_ptr())
        };
        if avail < 64 {
            let keep = (1u64 << avail) - 1;
            nl &= keep;
            cm &= keep;
            qt &= keep;
        }
        let mut seps = nl | cm;
        while seps != 0 {
            let pos = seps.trailing_zeros();
            let below = (1u64 << pos) - 1;
            quotes += (qt & below).count_ones();
            qt &= !below;
            qt &= !(1u64 << pos);
            let abs = base + pos as usize;
            if (nl >> pos) & 1 != 0 {
                emit(line_start, abs, nc, quotes, &mut ends);
                line_start = abs + 1;
                nc = 0;
                quotes = 0;
            } else {
                if nc < N - 1 {
                    ends[nc] = abs;
                }
                nc += 1;
            }
            seps &= seps - 1;
        }
        quotes += qt.count_ones();
        base += 64;
    }
    if line_start < n {
        emit(line_start, n, nc, quotes, &mut ends);
    }
}

// ---------------------------------------------------------------- hashing

/// Two independent 64-bit hashes of a byte string (together a 128-bit identity).
#[inline(always)]
fn hash2(b: &[u8]) -> (u64, u64) {
    const K1: u64 = 0x9E37_79B9_7F4A_7C15;
    const K2: u64 = 0xC2B2_AE3D_27D4_EB4F;
    let len = b.len() as u64;
    let mut a: u64 = len.wrapping_mul(0xFF51_AFD7_ED55_8CCD) ^ 0x2545_F491_4F6C_DD1D;
    let mut c: u64 = len.wrapping_mul(0xC4CE_B9FE_1A85_EC53) ^ 0x1656_67B1_9E37_79F9;
    let n = b.len();
    let mut i = 0;
    while i + 8 <= n {
        let v = load8(b, i);
        a = (a ^ v).wrapping_mul(K1);
        a ^= a >> 29;
        c = (c ^ v.rotate_left(23)).wrapping_mul(K2);
        c ^= c >> 31;
        i += 8;
    }
    if i < n {
        // the last 1..7 bytes, read without copying (overlapping loads; the length is already mixed in)
        let r = n - i;
        let v = if n >= 8 {
            load8(b, n - 8) >> (8 * (8 - r))
        } else if r >= 4 {
            let lo = u32::from_le_bytes(b[0..4].try_into().unwrap()) as u64;
            let hi = u32::from_le_bytes(b[r - 4..r].try_into().unwrap()) as u64;
            lo | hi << 32
        } else {
            b[0] as u64 | (b[r / 2] as u64) << 8 | (b[r - 1] as u64) << 16
        };
        a = (a ^ v).wrapping_mul(K1);
        a ^= a >> 29;
        c = (c ^ v.rotate_left(23)).wrapping_mul(K2);
        c ^= c >> 31;
    }
    a ^= a >> 32;
    a = a.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    a ^= a >> 32;
    c ^= c >> 33;
    c = c.wrapping_mul(0x9FB2_1C65_1E98_DF25);
    c ^= c >> 29;
    (a, c)
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

/// Small exact map from byte strings to u32 (products, campaigns).
struct StrMap {
    slots: Vec<(u64, S, u32)>,
    mask: usize,
}

impl StrMap {
    fn new(items: &[(S, u32)]) -> Self {
        let cap = (items.len() * 2).next_power_of_two().max(16);
        let mut m = StrMap { slots: vec![(0, &[][..], 0); cap], mask: cap - 1 };
        for &(k, v) in items {
            let h = hash2(k).0 | 1;
            let mut i = h as usize & m.mask;
            loop {
                if m.slots[i].0 == 0 {
                    m.slots[i] = (h, k, v);
                    break;
                }
                if m.slots[i].0 == h && m.slots[i].1 == k {
                    break; // first one wins
                }
                i = (i + 1) & m.mask;
            }
        }
        m
    }
    #[inline]
    fn get(&self, k: &[u8]) -> Option<u32> {
        let h = hash2(k).0 | 1;
        let mut i = h as usize & self.mask;
        loop {
            let s = &self.slots[i];
            if s.0 == 0 {
                return None;
            }
            if s.0 == h && s.1 == k {
                return Some(s.2);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- timestamps

#[cfg(test)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 { Some((a * 10 + c) as i64) } else { None }
}

/// Epoch ms; `BAD` if invalid; `i64::MAX` for an all-digit value too large for i64.
#[inline(always)]
fn parse_ts(b: &[u8]) -> i64 {
    let l = b.len();
    if l == 0 {
        return BAD;
    }
    if (l == 20 || l == 25) && b[4] == b'-' {
        return parse_cal(b).unwrap_or(BAD);
    }
    if (8..=16).contains(&l) {
        // last 8 digits by SWAR, the rest one at a time
        let tail = load8(b, l - 8);
        if !all_digits(tail) {
            return BAD;
        }
        let mut v: i64 = 0;
        for &c in &b[..l - 8] {
            let d = c.wrapping_sub(b'0');
            if d >= 10 {
                return BAD;
            }
            v = v * 10 + d as i64;
        }
        return v * 100_000_000 + digits8(tail) as i64;
    }
    if l <= 18 {
        let mut v: i64 = 0;
        for &c in b {
            let d = c.wrapping_sub(b'0');
            if d >= 10 {
                return BAD;
            }
            v = v * 10 + d as i64;
        }
        return v;
    }
    let mut v: i64 = 0;
    let mut over = false;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return BAD;
        }
        if !over {
            match v.checked_mul(10).and_then(|x| x.checked_add(d as i64)) {
                Some(x) => v = x,
                None => over = true,
            }
        }
    }
    if over { i64::MAX } else { v }
}

/// Days from 1970-01-01 to the first of month `mo` of year `y` (proleptic Gregorian).
#[inline(always)]
fn month_start(y: i64, mo: i64, leap: bool) -> i64 {
    const CUM: [i64; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let yp = y - 1 + 400; // keep the divisions on non-negative numbers (shifts the epoch by 400 years)
    let days_to_year = 365 * (y - 1970) + (yp / 4 - (1969 + 400) / 4) - (yp / 100 - (1969 + 400) / 100) + (yp / 400 - (1969 + 400) / 400);
    days_to_year + CUM[(mo - 1) as usize] + (leap && mo > 2) as i64
}

/// Every byte of `x` is an ASCII digit.
#[inline(always)]
fn all_digits(x: u64) -> bool {
    (x & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030 && (x.wrapping_add(0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030
}

/// Value of 8 ASCII digits (first byte most significant); caller checked `all_digits`.
#[inline(always)]
fn digits8(x: u64) -> u64 {
    let mut v = x & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v.wrapping_mul(2561)) >> 8;
    v = ((v & 0x00FF_00FF_00FF_00FF).wrapping_mul(6_553_601)) >> 16;
    ((v & 0x0000_FFFF_0000_FFFF).wrapping_mul(42_949_672_960_001)) >> 32
}

#[inline(always)]
fn parse_cal(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    // all 14 digit positions must be ASCII digits: check two 8-byte words with the separators masked to '0'
    let x0 = (load8(b, 0) & !0xFF00_00FF_0000_0000) | 0x3000_0030_0000_0000; // bytes 4 and 7
    let x1 = (load8(b, 8) & !0x0000_FF00_00FF_0000) | 0x0000_3000_0030_0000; // bytes 10 and 13
    let x2 = (b[17] as u64) | (b[18] as u64) << 8 | 0x3030_3030_3030_0000;
    if !all_digits(x0) || !all_digits(x1) || !all_digits(x2) {
        return None;
    }
    let g = |i: usize| b[i].wrapping_sub(b'0');
    let ds = [g(0), g(1), g(2), g(3), g(5), g(6), g(8), g(9), g(11), g(12), g(14), g(15), g(17), g(18)];
    let v = |i: usize, j: usize| (ds[i] as i64) * 10 + ds[j] as i64;
    let y = v(0, 1) * 100 + v(2, 3);
    let mo = v(4, 5);
    let d = v(6, 7);
    let h = v(8, 9);
    let mi = v(10, 11);
    let s = v(12, 13);
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let md = match mo {
        2 => 28 + leap as i64,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d < 1 || d > md {
        return None;
    }
    let mut off = 0;
    if b.len() == 20 {
        if b[19] != b'Z' {
            return None;
        }
    } else {
        let sg = b[19];
        if (sg != b'+' && sg != b'-') || b[22] != b':' {
            return None;
        }
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        off = (oh * 60 + om) * 60_000;
        if sg == b'-' {
            off = -off;
        }
    }
    Some((((month_start(y, mo, leap) + d - 1) * 24 + h) * 60 + mi) * 60_000 + s * 1000 - off)
}

/// The last calendar date parsed (bytes 0..10 of an F1/F2 timestamp) and its day number.
/// Rows in a chunk mostly share a few dates, so most timestamps only need their time parsed.
#[derive(Default, Clone, Copy)]
struct DateSlot {
    k: u64,
    k2: u16,
    day: i64,
}

/// Direct-mapped by day of month, so a window of up to 28 days never evicts.
#[derive(Default, Clone, Copy)]
struct DateCache {
    slots: [DateSlot; 32],
}

#[inline(always)]
fn parse_ts_c(b: &[u8], c: &mut DateCache) -> i64 {
    let l = b.len();
    if (l == 20 || l == 25) && b[4] == b'-' {
        return parse_cal_c(b, c).unwrap_or(BAD);
    }
    parse_ts(b)
}

#[inline(always)]
fn parse_cal_c(b: &[u8], c: &mut DateCache) -> Option<i64> {
    if b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let k = load8(b, 0);
    let k2 = u16::from_le_bytes([b[8], b[9]]);
    let si = (b[8] as usize * 10 + b[9] as usize) & 31;
    let slot = &mut c.slots[si];
    let day = if k == slot.k && k2 == slot.k2 {
        slot.day
    } else {
        // validate and compute the date: reuse the full parser on a midnight UTC copy
        let mut t = [0u8; 20];
        t[..10].copy_from_slice(&b[..10]);
        t[10..].copy_from_slice(b"T00:00:00Z");
        let ms = parse_cal(&t)?;
        let day = ms / DAY;
        *slot = DateSlot { k, k2, day };
        day
    };
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let mut off = 0;
    if b.len() == 20 {
        if b[19] != b'Z' {
            return None;
        }
    } else {
        let sg = b[19];
        if (sg != b'+' && sg != b'-') || b[22] != b':' {
            return None;
        }
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        off = (oh * 60 + om) * 60_000;
        if sg == b'-' {
            off = -off;
        }
    }
    Some(((day * 24 + h) * 60 + mi) * 60_000 + s * 1000 - off)
}

/// Digits only, 1..=10_000_000; 0 means invalid.
#[inline]
fn parse_price(b: &[u8]) -> u32 {
    if b.is_empty() {
        return 0;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return 0;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return 0;
        }
    }
    v as u32
}

// ---------------------------------------------------------------- records

/// Packs price (24 bits), category (16 bits) and a string length (24 bits).
#[inline(always)]
fn pack(price: u32, cat: u32, len: usize) -> u64 {
    debug_assert!(price < 1 << 24 && cat < 1 << 16);
    assert!(len < 1 << 24, "ID too long");
    price as u64 | (cat as u64) << 24 | (len as u64) << 40
}
#[inline(always)]
fn pk_price(pk: u64) -> u32 {
    (pk & 0xFF_FFFF) as u32
}
#[inline(always)]
fn pk_cat(pk: u64) -> u32 {
    ((pk >> 24) & 0xFFFF) as u32
}
#[inline(always)]
fn pk_str(ptr: *const u8, pk: u64) -> S {
    let len = (pk >> 40) as usize;
    if len == 0 { &[] } else { unsafe { std::slice::from_raw_parts(ptr, len) } }
}

/// One parsed event copy (64 bytes). `uptr`/len are null/0 if the copy fails validation or is late.
/// The event_id is identified by its 128-bit hash (h, h2).
#[derive(Clone, Copy)]
struct RawEv {
    h: u64,
    h2: u64,
    ing: i64,
    ts: i64,
    u1: u64,
    u2: u64,
    uptr: *const u8,
    pk: u64,
}
unsafe impl Send for RawEv {}
unsafe impl Sync for RawEv {}

#[derive(Clone, Copy)]
struct RawCl {
    uh: (u64, u64),
    ts: i64,
    cid: S,
    camp: u32,
}

/// A surviving event, keyed by person identity (40 bytes).
#[derive(Clone, Copy)]
struct Ev {
    p1: u64,
    p2: u64,
    ts: i64,
    nptr: *const u8,
    pk: u64,
}
unsafe impl Send for Ev {}
unsafe impl Sync for Ev {}

#[derive(Clone, Copy)]
struct Cl {
    p1: u64,
    p2: u64,
    ts: i64,
    cid: S,
    camp: u32,
}

struct LinkSide {
    h: (u64, u64),
    s: S,
    /// index of this side among its parsing thread's sides (pair k has sides 2k and 2k+1)
    r: u32,
}

struct Ctx {
    ws: i64,
    we: i64,
    prod: StrMap,
    camp: StrMap,
    unknown_cat: u32,
}

#[inline(always)]
fn part_of(h: u64) -> usize {
    (h >> (64 - PBITS)) as usize
}

fn parse_events(ctx: &Ctx, chunk: S, skip: bool, out: &mut [Vec<RawEv>]) {
    let mut c_ing = DateCache::default();
    let mut c_ts = DateCache::default();
    for_records::<7>(chunk, skip, |f| {
        let eid = f[0];
        if eid.is_empty() {
            return;
        }
        let (h, h2) = hash2(eid);
        // ingest_ts is always valid F1 per the rules; an invalid one can never be the kept copy
        let ing = {
            let x = parse_ts_c(f[3], &mut c_ing);
            if x == BAD { i64::MAX } else { x }
        };
        let mut r = RawEv { h, h2, ing, ts: 0, u1: 0, u2: 0, uptr: std::ptr::null(), pk: 0 };
        'v: {
            if f[1].is_empty() || f[5].is_empty() {
                break 'v;
            }
            let t = parse_ts_c(f[2], &mut c_ts);
            if t == BAD || t < ctx.ws || t >= ctx.we || ing == i64::MAX || ing - t > DAY {
                break 'v;
            }
            let (mut price, mut cat) = (0, 0);
            let et = f[4];
            if et.len() == 4 {
                let w = u32::from_le_bytes(et[..4].try_into().unwrap());
                if w != u32::from_le_bytes(*b"view") && w != u32::from_le_bytes(*b"cart") {
                    break 'v;
                }
            } else if et.len() == 8 && load8(et, 0) == u64::from_le_bytes(*b"purchase") {
                price = parse_price(f[6]);
                if price == 0 {
                    break 'v;
                }
                cat = ctx.prod.get(f[5]).unwrap_or(ctx.unknown_cat);
            } else {
                break 'v;
            }
            r.ts = t;
            (r.u1, r.u2) = hash2(f[1]);
            r.uptr = f[1].as_ptr();
            r.pk = pack(price, cat, f[1].len());
        }
        out[part_of(h)].push(r);
    });
}

fn parse_clicks(ctx: &Ctx, chunk: S, skip: bool, out: &mut Vec<RawCl>) {
    let lo = ctx.ws - WEEK;
    let mut c_ts = DateCache::default();
    for_records::<4>(chunk, skip, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let t = parse_ts_c(f[3], &mut c_ts);
        if t == BAD || t < lo || t >= ctx.we {
            return;
        }
        let Some(camp) = ctx.camp.get(f[2]) else {
            return;
        };
        out.push(RawCl { uh: hash2(f[1]), ts: t, cid: f[0], camp });
    });
}

fn parse_links(chunk: S, skip: bool, out: &mut [Vec<LinkSide>], nsides: &mut u32) {
    for_records::<2>(chunk, skip, |f| {
        if !f[0].is_empty() && !f[1].is_empty() {
            for v in [f[0], f[1]] {
                let h = hash2(v);
                out[part_of(h.0)].push(LinkSide { h, s: v, r: *nsides });
                *nsides += 1;
            }
        }
    });
}

fn read_lookup(path: &Path) -> Vec<(S, S)> {
    let data = map_file(path);
    let mut v = Vec::new();
    for_records::<2>(data, true, |f| {
        if !f[0].is_empty() {
            v.push((f[0], f[1]));
        }
    });
    v
}

fn manifest_value(text: &str, key: &str) -> i64 {
    let at = text.find(&format!("\"{key}\"")).expect("manifest key");
    let rest = &text[at + key.len() + 2..];
    let rest = &rest[rest.find(':').unwrap() + 1..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    let v = parse_ts(rest[..q2].as_bytes());
    assert!(v != BAD && v != i64::MAX, "bad manifest time");
    v
}

/// Run tasks 0..n over `threads` workers; each worker gets its own state from `init`.
fn par<T: Send, F: Fn(&mut T, usize) + Sync>(threads: usize, n: usize, init: impl Fn() -> T + Sync, f: F) -> Vec<T> {
    let next = AtomicUsize::new(0);
    let out = Mutex::new(Vec::new());
    std::thread::scope(|sc| {
        for _ in 0..threads.min(n).max(1) {
            sc.spawn(|| {
                let mut st = init();
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    f(&mut st, i);
                }
                out.lock().unwrap().push(st);
            });
        }
    });
    out.into_inner().unwrap()
}

// ---------------------------------------------------------------- people

/// Linked IDs: 128-bit identity -> person (128-bit identity of the person's name, and the name).
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct LinkSlot {
    k1: u64,
    k2: u64,
    p1: u64,
    p2: u64,
    name: S,
}

/// One open-addressing table per bucket (`part_of` of the ID's first hash).
struct People {
    tables: Vec<(Vec<LinkSlot>, usize)>,
}

impl People {
    #[inline(always)]
    fn slot_ptr(&self, h1: u64) -> *const LinkSlot {
        let (t, mask) = &self.tables[part_of(h1)];
        &t[(h1 | 1) as usize & mask] as *const _
    }
    #[inline(always)]
    fn person(&self, h: (u64, u64), user: S) -> (u64, u64, S) {
        let (t, mask) = &self.tables[part_of(h.0)];
        let k1 = h.0 | 1;
        let mut i = k1 as usize & mask;
        loop {
            let s = &t[i];
            if s.k1 == 0 {
                return (h.0, h.1, user);
            }
            if s.k1 == k1 && s.k2 == h.1 {
                return (s.p1, s.p2, s.name);
            }
            i = (i + 1) & mask;
        }
    }
}

#[inline(always)]
fn name_prefix(s: &[u8]) -> u64 {
    let mut t = [0u8; 8];
    let l = s.len().min(8);
    t[..l].copy_from_slice(&s[..l]);
    u64::from_be_bytes(t)
}

/// Lock-free union-find: roots only ever link to a smaller index.
fn uf_find(parent: &[std::sync::atomic::AtomicU32], mut x: u32) -> u32 {
    loop {
        let p = parent[x as usize].load(Ordering::Relaxed);
        if p == x {
            return x;
        }
        let gp = parent[p as usize].load(Ordering::Relaxed);
        if gp != p {
            let _ = parent[x as usize].compare_exchange(p, gp, Ordering::Relaxed, Ordering::Relaxed);
        }
        x = p;
    }
}

fn uf_union(parent: &[std::sync::atomic::AtomicU32], a: u32, b: u32) {
    loop {
        let ra = uf_find(parent, a);
        let rb = uf_find(parent, b);
        if ra == rb {
            return;
        }
        let (hi, lo) = if ra > rb { (ra, rb) } else { (rb, ra) };
        if parent[hi as usize].compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            return;
        }
    }
}

/// sides[t][q]: link sides parsed by thread t that fall in bucket q; nsides[t]: sides of thread t.
fn build_people(threads: usize, sides: &[Vec<Vec<LinkSide>>], nsides: &[u32]) -> People {
    use std::sync::atomic::AtomicU32;
    // L1: intern per bucket; side -> (bucket, local id)
    let side_ids: Vec<Vec<AtomicU32>> = nsides.iter().map(|&n| (0..n).map(|_| AtomicU32::new(0)).collect()).collect();
    let tables: Vec<Mutex<(Vec<LinkSlot>, usize, Vec<S>)>> = (0..P).map(|_| Mutex::new((Vec::new(), 0, Vec::new()))).collect();
    par(threads, P, || (), |_, q| {
        let n: usize = sides.iter().map(|t| t[q].len()).sum();
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        let empty = LinkSlot { k1: 0, k2: 0, p1: 0, p2: 0, name: &[] };
        let mut slots = vec![empty; cap];
        let mut names: Vec<S> = Vec::new();
        for (t, list) in sides.iter().enumerate() {
            for side in &list[q] {
                let k1 = side.h.0 | 1;
                let mut i = k1 as usize & mask;
                let local = loop {
                    let sl = &mut slots[i];
                    if sl.k1 == 0 {
                        let id = names.len() as u64;
                        *sl = LinkSlot { k1, k2: side.h.1, p1: id, p2: 0, name: side.s };
                        names.push(side.s);
                        break id;
                    }
                    if sl.k1 == k1 && sl.k2 == side.h.1 {
                        break sl.p1;
                    }
                    i = (i + 1) & mask;
                };
                side_ids[t][side.r as usize].store(((q as u32) << 23) | local as u32, Ordering::Relaxed);
            }
        }
        assert!(names.len() < (1 << 23));
        *tables[q].lock().unwrap() = (slots, mask, names);
    });
    let mut tables: Vec<(Vec<LinkSlot>, usize, Vec<S>)> = tables.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let mut base = vec![0u32; P + 1];
    for q in 0..P {
        base[q + 1] = base[q] + tables[q].2.len() as u32;
    }
    let nid = base[P] as usize;
    let dense = |packed: u32| base[(packed >> 23) as usize] + (packed & ((1 << 23) - 1));
    let mut id_name: Vec<S> = Vec::with_capacity(nid);
    for t in &mut tables {
        id_name.extend(std::mem::take(&mut t.2));
    }
    let id_prefix: Vec<u64> = id_name.iter().map(|s| name_prefix(s)).collect();
    // L2: union over all pairs
    let parent: Vec<AtomicU32> = (0..nid as u32).map(AtomicU32::new).collect();
    par(threads, side_ids.len(), || (), |_, t| {
        let ids = &side_ids[t];
        for k in 0..ids.len() / 2 {
            let a = dense(ids[2 * k].load(Ordering::Relaxed));
            let b = dense(ids[2 * k + 1].load(Ordering::Relaxed));
            uf_union(&parent, a, b);
        }
    });
    // L3: the byte-wise smallest name of every component
    let best: Vec<AtomicU32> = (0..nid).map(|_| AtomicU32::new(u32::MAX)).collect();
    let less = |a: u32, b: u32| -> bool {
        let (pa, pb) = (id_prefix[a as usize], id_prefix[b as usize]);
        if pa != pb { pa < pb } else { id_name[a as usize] < id_name[b as usize] }
    };
    const BLK: usize = 1 << 14;
    let nblk = nid.div_ceil(BLK);
    par(threads, nblk, || (), |_, bi| {
        for i in bi * BLK..((bi + 1) * BLK).min(nid) {
            let r = uf_find(&parent, i as u32) as usize;
            let mut cur = best[r].load(Ordering::Relaxed);
            loop {
                if cur != u32::MAX && !less(i as u32, cur) {
                    break;
                }
                match best[r].compare_exchange(cur, i as u32, Ordering::AcqRel, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(x) => cur = x,
                }
            }
        }
    });
    // L4: fill every slot with its person
    let tables: Vec<Mutex<(Vec<LinkSlot>, usize, Vec<S>)>> = tables.into_iter().map(Mutex::new).collect();
    par(threads, P, || (), |_, q| {
        let mut g = tables[q].lock().unwrap();
        let b = base[q];
        for sl in g.0.iter_mut() {
            if sl.k1 != 0 {
                let id = b + sl.p1 as u32;
                let r = uf_find(&parent, id) as usize;
                let name = id_name[best[r].load(Ordering::Relaxed) as usize];
                let (p1, p2) = hash2(name);
                sl.p1 = p1;
                sl.p2 = p2;
                sl.name = name;
            }
        }
    });
    People { tables: tables.into_iter().map(|m| { let (s, mask, _) = m.into_inner().unwrap(); (s, mask) }).collect() }
}

// ---------------------------------------------------------------- aggregation

#[derive(Default)]
struct UserRows {
    name: Vec<u8>,
    name_end: Vec<u32>,
    day: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    /// category ids of the rows that have a top category
    top: Vec<u32>,
    top_present: Vec<bool>,
}

struct Agg {
    rows: UserRows,
    /// per (campaign * ndays + day): sessions, purchasing sessions, revenue, assisted sessions
    camp: Vec<[i64; 4]>,
    cnt: Vec<u32>,
    inlist: Vec<bool>,
    active: Vec<u32>,
    catsum: Vec<i64>,
    cattouched: Vec<u32>,
    // scratch for grouping a bucket by person
    pmap: Vec<(u64, u64, u32)>,
    ids: Vec<u32>,
    counts: Vec<u32>,
    offs: Vec<u32>,
    cursor: Vec<u32>,
    evs: Vec<Ev>,
    cids: Vec<u32>,
    ccounts: Vec<u32>,
    coffs: Vec<u32>,
    cls: Vec<Cl>,
}

fn flush_day(ag: &mut Agg, name: S, day: i64, ds: i64, de: i64, dr: i64) {
    let r = &mut ag.rows;
    r.name.extend_from_slice(name);
    r.name_end.push(r.name.len() as u32);
    r.day.push(day as i32);
    r.sessions.push(ds);
    r.events.push(de);
    r.revenue.push(dr);
    if ag.cattouched.is_empty() {
        r.top_present.push(false);
    } else {
        let mut best = u32::MAX;
        let mut bv = -1i64;
        for &c in &ag.cattouched {
            let v = ag.catsum[c as usize];
            if v > bv || (v == bv && c < best) {
                bv = v;
                best = c;
            }
            ag.catsum[c as usize] = 0;
        }
        ag.cattouched.clear();
        r.top_present.push(true);
        r.top.push(best);
    }
}

/// One person's events (sorted by ts) and valid clicks (sorted by ts).
#[inline]
fn person_sessions(ag: &mut Agg, evs: &[Ev], cls: &[Cl], ws_day: i64, ndays: usize, organic: u32) {
    let name = pk_str(evs[0].nptr, evs[0].pk);
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let (mut ds, mut de, mut dr) = (0i64, 0i64, 0i64);
    let n = evs.len();
    let mut i = 0;
    while i < n {
        let start = evs[i].ts;
        let mut j = i + 1;
        let mut prev = start;
        while j < n && evs[j].ts - prev <= GAP {
            prev = evs[j].ts;
            j += 1;
        }
        let day = start.div_euclid(DAY);
        if day != cur_day {
            if cur_day != i64::MIN {
                flush_day(ag, name, cur_day, ds, de, dr);
            }
            cur_day = day;
            ds = 0;
            de = 0;
            dr = 0;
        }
        let mut rev = 0i64;
        let mut purchase = false;
        for e in &evs[i..j] {
            let price = pk_price(e.pk);
            if price > 0 {
                purchase = true;
                rev += price as i64;
                let c = pk_cat(e.pk);
                if ag.catsum[c as usize] == 0 {
                    ag.cattouched.push(c);
                }
                ag.catsum[c as usize] += price as i64;
            }
        }
        ds += 1;
        de += (j - i) as i64;
        dr += rev;

        // clicks with start - 7 days <= ts <= start
        while hi < cls.len() && cls[hi].ts <= start {
            let c = cls[hi].camp as usize;
            ag.cnt[c] += 1;
            if !ag.inlist[c] {
                ag.inlist[c] = true;
                ag.active.push(c as u32);
            }
            hi += 1;
        }
        while lo < hi && cls[lo].ts < start - WEEK {
            ag.cnt[cls[lo].camp as usize] -= 1;
            lo += 1;
        }
        let d = (day - ws_day) as usize;
        let credited = if lo < hi {
            // latest ts; on a tie the byte-wise smallest click_id
            let t = cls[hi - 1].ts;
            let mut best = hi - 1;
            let mut k = hi - 1;
            while k > lo && cls[k - 1].ts == t {
                k -= 1;
                if cls[k].cid < cls[best].cid {
                    best = k;
                }
            }
            let (cnt, inlist, camp) = (&ag.cnt, &mut ag.inlist, &mut ag.camp);
            ag.active.retain(|&c| {
                if cnt[c as usize] > 0 {
                    camp[c as usize * ndays + d][3] += 1;
                    true
                } else {
                    inlist[c as usize] = false;
                    false
                }
            });
            cls[best].camp
        } else {
            organic
        };
        let x = &mut ag.camp[credited as usize * ndays + d];
        x[0] += 1;
        x[1] += purchase as i64;
        x[2] += rev;
        i = j;
    }
    flush_day(ag, name, cur_day, ds, de, dr);
    for &c in &ag.active {
        ag.cnt[c as usize] = 0;
        ag.inlist[c as usize] = false;
    }
    ag.active.clear();
}

// ---------------------------------------------------------------- main

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let pos: Vec<&String> = args.iter().skip(1).filter(|a| !a.starts_with("--")).collect();
    let data_dir = PathBuf::from(pos.first().map(|s| s.to_string()).unwrap_or_else(|| std::env::var("DATA_DIR").unwrap_or("/data".into())));
    let out_dir = PathBuf::from(pos.get(1).map(|s| s.to_string()).unwrap_or_else(|| std::env::var("OUTPUT_DIR").unwrap_or("/output".into())));
    let csv = args.iter().any(|a| a == "--csv");
    if args.iter().any(|a| a == "--csv-selftest") {
        use std::io::{Read, Write};
        let mut v = Vec::new();
        std::io::stdin().read_to_end(&mut v).unwrap();
        let data: S = Box::leak(v.into_boxed_slice());
        let mut out = Vec::new();
        for_records::<4>(data, true, |f| {
            for (k, x) in f.iter().enumerate() {
                if k > 0 { out.push(1u8); }
                out.extend_from_slice(x);
            }
            out.push(2u8);
        });
        std::io::stdout().write_all(&out).unwrap();
        return;
    }
    if args.iter().any(|a| a == "--ts-selftest") {
        // read lines from stdin, print parse_ts results (for testing against the reference)
        use std::io::BufRead;
        let mut cache = DateCache::default();
        for line in std::io::stdin().lock().split(b'\n') {
            let line = line.unwrap();
            let v = parse_ts_c(&line, &mut cache);
            if v == BAD { println!("BAD"); } else if v == i64::MAX { println!("HUGE"); } else { println!("{v}"); }
        }
        return;
    }
    let timing = std::env::var("SS_TIMING").is_ok();
    let t0 = std::time::Instant::now();
    let lap = |what: &str| {
        if timing {
            eprintln!("{:>8.3}s {what}", t0.elapsed().as_secs_f64());
        }
    };
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()));

    // ---- lookups
    let man = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = manifest_value(&man, "window_start");
    let we = manifest_value(&man, "window_end");
    let products = read_lookup(&data_dir.join("products.csv"));
    let campaigns = read_lookup(&data_dir.join("campaigns.csv"));
    let mut cats: Vec<S> = products.iter().map(|p| p.1).collect();
    cats.push(b"unknown");
    cats.sort_unstable();
    cats.dedup();
    let cat_id = |c: &[u8]| cats.binary_search_by(|x| (*x).cmp(c)).unwrap() as u32;
    let prod = StrMap::new(&products.iter().map(|&(p, c)| (p, cat_id(c))).collect::<Vec<_>>());
    let mut camp_names: Vec<(S, S)> = Vec::new();
    for &(c, ch) in &campaigns {
        if !camp_names.iter().any(|x| x.0 == c) || campaigns.len() > 10_000 {
            camp_names.push((c, ch));
        }
    }
    let camp = StrMap::new(&camp_names.iter().enumerate().map(|(i, &(c, _))| (c, i as u32)).collect::<Vec<_>>());
    let ncamp = camp_names.len();
    let ctx = Ctx { ws, we, prod, camp, unknown_cat: cat_id(b"unknown") };
    let ws_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - ws_day + 1).max(1) as usize;
    lap("lookups");

    // ---- A: parse
    const CHUNK: usize = 2 << 20;
    let mut tasks: Vec<(u8, S, bool)> = Vec::new();
    for (kind, dir) in [(2u8, "links"), (0, "events"), (1, "clicks")] {
        let mut ch = Vec::new();
        for p in csv_files(&data_dir.join(dir)) {
            chunks_of(map_file(&p), CHUNK, &mut ch);
        }
        tasks.extend(ch.into_iter().map(|(c, s)| (kind, c, s)));
    }
    let ev_bytes: usize = tasks.iter().filter(|t| t.0 == 0).map(|t| t.1.len()).sum();
    let cl_bytes: usize = tasks.iter().filter(|t| t.0 == 1).map(|t| t.1.len()).sum();
    let ev_res = ev_bytes / 70 / P / threads + 16;
    let cl_res = cl_bytes / 45 / threads + 16;
    struct AState {
        ev: Vec<Vec<RawEv>>,
        cl: Vec<RawCl>,
        ln: Vec<Vec<LinkSide>>,
        nsides: u32,
    }
    let astates = par(
        threads,
        tasks.len(),
        || AState { ev: (0..P).map(|_| Vec::with_capacity(ev_res)).collect(), cl: Vec::with_capacity(cl_res), ln: (0..P).map(|_| Vec::new()).collect(), nsides: 0 },
        |st, i| {
            let (kind, chunk, skip) = tasks[i];
            match kind {
                0 => parse_events(&ctx, chunk, skip, &mut st.ev),
                1 => parse_clicks(&ctx, chunk, skip, &mut st.cl),
                _ => parse_links(chunk, skip, &mut st.ln, &mut st.nsides),
            }
        },
    );
    lap("parse");

    // ---- L: people
    let mut astates = astates;
    let sides: Vec<Vec<Vec<LinkSide>>> = astates.iter_mut().map(|a| std::mem::take(&mut a.ln)).collect();
    let nsides: Vec<u32> = astates.iter().map(|a| a.nsides).collect();
    let people = build_people(threads, &sides, &nsides);
    drop(sides);
    lap("links");

    // ---- B: dedup events; map events and clicks to people
    struct BState {
        ev: Vec<Vec<Ev>>,
        cl: Vec<Vec<Cl>>,
        win: Vec<u64>,
        map: Vec<(u64, u32)>,
        t: [f64; 3],
    }
    let nb = P + astates.len();
    let n_ev: usize = astates.iter().map(|a| a.ev.iter().map(|v| v.len()).sum::<usize>()).sum();
    let n_cl: usize = astates.iter().map(|a| a.cl.len()).sum();
    let ev_res2 = n_ev / threads / P * 5 / 4 + 16;
    let cl_res2 = n_cl / threads / P * 5 / 4 + 16;
    let bstates = par(
        threads,
        nb,
        || BState {
            ev: (0..P).map(|_| Vec::with_capacity(ev_res2)).collect(),
            cl: (0..P).map(|_| Vec::with_capacity(cl_res2)).collect(),
            win: Vec::new(),
            map: Vec::new(),
            t: [0.0; 3],
        },
        |st, i| {
            if i < P {
                let t_0 = std::time::Instant::now();
                let total: usize = astates.iter().map(|a| a.ev[i].len()).sum();
                let cap = (total * 2).next_power_of_two().max(16);
                let mask = cap - 1;
                st.map.clear();
                st.map.resize(cap, (0, 0));
                // winners as (source, index) packed: source << 32 | index
                st.win.clear();
                st.win.reserve(total);
                for (src, a) in astates.iter().enumerate() {
                    let v = &a.ev[i];
                    for (idx, r) in v.iter().enumerate() {
                        let k = r.h | 1;
                        let mut s = k as usize & mask;
                        loop {
                            let (mk, mv) = st.map[s];
                            if mk == 0 {
                                st.map[s] = (k, st.win.len() as u32);
                                st.win.push(((src as u64) << 32) | idx as u64);
                                break;
                            }
                            if mk == k {
                                let w = st.win[mv as usize];
                                let b = &astates[(w >> 32) as usize].ev[i][(w & 0xFFFF_FFFF) as usize];
                                if b.h2 == r.h2 {
                                    if r.ing < b.ing {
                                        st.win[mv as usize] = ((src as u64) << 32) | idx as u64;
                                    }
                                    break;
                                }
                            }
                            s = (s + 1) & mask;
                        }
                    }
                }
                st.t[0] += t_0.elapsed().as_secs_f64();
                let t_1 = std::time::Instant::now();
                const AHEAD: usize = 32;
                let get = |w: u64| &astates[(w >> 32) as usize].ev[i][(w & 0xFFFF_FFFF) as usize];
                let win = &st.win;
                for (j, &w) in win.iter().enumerate() {
                    if let Some(&nx) = win.get(j + AHEAD) {
                        prefetch(people.slot_ptr(get(nx).u1));
                    }
                    let r = get(w);
                    if r.uptr.is_null() {
                        continue;
                    }
                    let (p1, p2, name) = people.person((r.u1, r.u2), pk_str(r.uptr, r.pk));
                    st.ev[part_of(p1)].push(Ev { p1, p2, ts: r.ts, nptr: name.as_ptr(), pk: (r.pk & 0xFF_FFFF_FFFF) | (name.len() as u64) << 40 });
                }
                st.t[1] += t_1.elapsed().as_secs_f64();
            } else {
                let t_2 = std::time::Instant::now();
                let cl = &astates[i - P].cl;
                const AHEAD: usize = 32;
                for (j, c) in cl.iter().enumerate() {
                    if let Some(nx) = cl.get(j + AHEAD) {
                        prefetch(people.slot_ptr(nx.uh.0));
                    }
                    let (p1, p2, _) = people.person(c.uh, &[]);
                    st.cl[part_of(p1)].push(Cl { p1, p2, ts: c.ts, cid: c.cid, camp: c.camp });
                }
                st.t[2] += t_2.elapsed().as_secs_f64();
            }
        },
    );
    drop(astates);
    if timing { for b in &bstates { eprintln!("   B thread: dedup {:.3} map {:.3} clicks {:.3}", b.t[0], b.t[1], b.t[2]); } }
    lap("dedup+people");

    // ---- C: sessions per person bucket
    let ncat = cats.len();
    assert!(ncat < 1 << 16, "too many categories");
    let organic = ncamp as u32;
    let aggs = par(
        threads,
        P,
        || Agg {
            rows: UserRows::default(),
            camp: vec![[0; 4]; (ncamp + 1) * ndays],
            cnt: vec![0; ncamp],
            inlist: vec![false; ncamp],
            active: Vec::new(),
            catsum: vec![0; ncat],
            cattouched: Vec::new(),
            pmap: Vec::new(),
            ids: Vec::new(),
            counts: Vec::new(),
            offs: Vec::new(),
            cursor: Vec::new(),
            evs: Vec::new(),
            cids: Vec::new(),
            ccounts: Vec::new(),
            coffs: Vec::new(),
            cls: Vec::new(),
        },
        |ag, q| {
            // dense person ids for this bucket, then a counting sort by person
            let total: usize = bstates.iter().map(|b| b.ev[q].len()).sum();
            if total == 0 {
                return;
            }
            let cap = (total / 2).next_power_of_two().max(64);
            ag.pmap.clear();
            ag.pmap.resize(cap, (0, 0, 0));
            let mut mask = cap - 1;
            ag.ids.clear();
            ag.counts.clear();
            let mut npeople = 0u32;
            for b in &bstates {
                for e in &b.ev[q] {
                    let k1 = e.p1 | 1;
                    let mut i = (k1 >> 7) as usize & mask;
                    let id = loop {
                        let sl = ag.pmap[i];
                        if sl.0 == 0 {
                            ag.pmap[i] = (k1, e.p2, npeople);
                            npeople += 1;
                            ag.counts.push(0);
                            if (npeople as usize) * 2 > ag.pmap.len() {
                                // grow and rehash
                                let old = std::mem::take(&mut ag.pmap);
                                let ncap = old.len() * 2;
                                ag.pmap.resize(ncap, (0, 0, 0));
                                mask = ncap - 1;
                                for o in old {
                                    if o.0 != 0 {
                                        let mut j = (o.0 >> 7) as usize & mask;
                                        while ag.pmap[j].0 != 0 {
                                            j = (j + 1) & mask;
                                        }
                                        ag.pmap[j] = o;
                                    }
                                }
                            }
                            break npeople - 1;
                        }
                        if sl.0 == k1 && sl.1 == e.p2 {
                            break sl.2;
                        }
                        i = (i + 1) & mask;
                    };
                    ag.ids.push(id);
                    ag.counts[id as usize] += 1;
                }
            }
            let np = npeople as usize;
            // offsets
            ag.offs.clear();
            ag.offs.reserve(np + 1);
            let mut acc = 0u32;
            for &c in &ag.counts {
                ag.offs.push(acc);
                acc += c;
            }
            ag.offs.push(acc);
            ag.evs.clear();
            ag.evs.reserve(total);
            unsafe { ag.evs.set_len(total) };
            ag.cursor.clear();
            ag.cursor.extend_from_slice(&ag.offs[..np]);
            let mut k = 0usize;
            for b in &bstates {
                for e in &b.ev[q] {
                    let id = ag.ids[k] as usize;
                    k += 1;
                    let at = ag.cursor[id] as usize;
                    ag.cursor[id] += 1;
                    ag.evs[at] = *e;
                }
            }
            // clicks of the people present
            ag.cids.clear();
            ag.ccounts.clear();
            ag.ccounts.resize(np, 0);
            let mut ctotal = 0usize;
            for b in &bstates {
                for c in &b.cl[q] {
                    let k1 = c.p1 | 1;
                    let mut i = (k1 >> 7) as usize & mask;
                    let id = loop {
                        let sl = ag.pmap[i];
                        if sl.0 == 0 {
                            break u32::MAX;
                        }
                        if sl.0 == k1 && sl.1 == c.p2 {
                            break sl.2;
                        }
                        i = (i + 1) & mask;
                    };
                    ag.cids.push(id);
                    if id != u32::MAX {
                        ag.ccounts[id as usize] += 1;
                        ctotal += 1;
                    }
                }
            }
            ag.coffs.clear();
            let mut acc = 0u32;
            for &c in &ag.ccounts {
                ag.coffs.push(acc);
                acc += c;
            }
            ag.coffs.push(acc);
            ag.cls.clear();
            ag.cls.reserve(ctotal);
            unsafe { ag.cls.set_len(ctotal) };
            ag.cursor.clear();
            ag.cursor.extend_from_slice(&ag.coffs[..np]);
            let mut k = 0usize;
            for b in &bstates {
                for c in &b.cl[q] {
                    let id = ag.cids[k];
                    k += 1;
                    if id != u32::MAX {
                        let at = ag.cursor[id as usize] as usize;
                        ag.cursor[id as usize] += 1;
                        ag.cls[at] = *c;
                    }
                }
            }
            let mut evs = std::mem::take(&mut ag.evs);
            let mut cls = std::mem::take(&mut ag.cls);
            for p in 0..np {
                let (es, ee) = (ag.offs[p] as usize, ag.offs[p + 1] as usize);
                let (cs, ce) = (ag.coffs[p] as usize, ag.coffs[p + 1] as usize);
                let ev = &mut evs[es..ee];
                ev.sort_unstable_by_key(|e| e.ts);
                let cl = &mut cls[cs..ce];
                cl.sort_unstable_by_key(|c| c.ts);
                person_sessions(ag, ev, cl, ws_day, ndays, organic);
            }
            ag.evs = evs;
            ag.cls = cls;
        },
    );
    drop(bstates);
    lap("sessions");

    // ---- W: write
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let mut camp_tot = vec![[0i64; 4]; (ncamp + 1) * ndays];
    for a in &aggs {
        for (t, x) in camp_tot.iter_mut().zip(&a.camp) {
            for k in 0..4 {
                t[k] += x[k];
            }
        }
    }
    let cats_ref = &cats;
    std::thread::scope(|sc| {
        for (t, a) in aggs.into_iter().enumerate() {
            let ud = ud.clone();
            sc.spawn(move || {
                let r = a.rows;
                if r.day.is_empty() && t > 0 {
                    return;
                }
                let mut top = Vec::new();
                let mut top_end = Vec::with_capacity(r.top.len());
                for &cid in &r.top {
                    top.extend_from_slice(cats_ref[cid as usize]);
                    top_end.push(top.len() as u32);
                }
                if csv {
                    write_user_csv(&ud.join(format!("part-{t:03}.csv")), &r, &top, &top_end);
                    return;
                }
                parquet::write_file(
                    &ud.join(format!("part-{t:03}.parquet")),
                    &[
                        ("user_id", Col::Str(r.name, r.name_end)),
                        ("day", Col::Date(r.day)),
                        ("sessions", Col::I64(r.sessions)),
                        ("events", Col::I64(r.events)),
                        ("revenue_cents", Col::I64(r.revenue)),
                        ("top_category", Col::OptStr(top, top_end, r.top_present)),
                    ],
                )
                .unwrap();
            });
        }
        let mut id = Vec::new();
        let mut id_end = Vec::new();
        let mut ch = Vec::new();
        let mut ch_end = Vec::new();
        let mut day = Vec::new();
        let mut cols: [Vec<i64>; 4] = Default::default();
        for c in 0..=ncamp {
            for d in 0..ndays {
                let x = camp_tot[c * ndays + d];
                if x[0] == 0 && x[3] == 0 {
                    continue;
                }
                let (n, chn): (S, S) = if c == ncamp { (b"organic", b"organic") } else { camp_names[c] };
                id.extend_from_slice(n);
                id_end.push(id.len() as u32);
                ch.extend_from_slice(chn);
                ch_end.push(ch.len() as u32);
                day.push((ws_day + d as i64) as i32);
                for k in 0..4 {
                    cols[k].push(x[k]);
                }
            }
        }
        if csv {
            let mut s = Vec::from(&b"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n"[..]);
            let (mut a, mut b) = (0usize, 0usize);
            for i in 0..day.len() {
                let (e1, e2) = (id_end[i] as usize, ch_end[i] as usize);
                s.extend_from_slice(&id[a..e1]);
                s.push(b',');
                s.extend_from_slice(&ch[b..e2]);
                s.extend_from_slice(format!(",{},{},{},{},{}\n", date_str(day[i]), cols[0][i], cols[1][i], cols[2][i], cols[3][i]).as_bytes());
                a = e1;
                b = e2;
            }
            std::fs::write(cd.join("part-000.csv"), s).unwrap();
        } else {
            let [s, ps, rv, asst] = cols;
            parquet::write_file(
                &cd.join("part-000.parquet"),
                &[
                    ("campaign_id", Col::Str(id, id_end)),
                    ("channel", Col::Str(ch, ch_end)),
                    ("day", Col::Date(day)),
                    ("sessions", Col::I64(s)),
                    ("purchasing_sessions", Col::I64(ps)),
                    ("revenue_cents", Col::I64(rv)),
                    ("assisted_sessions", Col::I64(asst)),
                ],
            )
            .unwrap();
        }
    });
    lap("write");
}

fn date_str(d: i32) -> String {
    let z = d as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let dd = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{y:04}-{m:02}-{dd:02}")
}

fn write_user_csv(path: &Path, r: &UserRows, top: &[u8], top_end: &[u32]) {
    let mut out: Vec<u8> = b"user_id,day,sessions,events,revenue_cents,top_category\n".to_vec();
    let (mut a, mut k, mut tb) = (0usize, 0usize, 0usize);
    for i in 0..r.day.len() {
        let e = r.name_end[i] as usize;
        out.extend_from_slice(&r.name[a..e]);
        a = e;
        out.extend_from_slice(format!(",{},{},{},{},", date_str(r.day[i]), r.sessions[i], r.events[i], r.revenue[i]).as_bytes());
        if r.top_present[i] {
            let te = top_end[k] as usize;
            out.extend_from_slice(&top[tb..te]);
            tb = te;
            k += 1;
        }
        out.push(b'\n');
    }
    std::fs::write(path, out).unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn month_start_matches_civil() {
        for y in [1, 4, 100, 400, 1600, 1900, 1969, 1970, 1999, 2000, 2024, 2025, 2028, 2100, 2400, 9999] {
            for mo in 1..=12 {
                let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
                assert_eq!(month_start(y, mo, leap), days_from_civil(y, mo, 1), "{y}-{mo}");
            }
        }
    }
}
