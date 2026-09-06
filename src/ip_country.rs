//! Built-in IP -> country table, compiled into the binary.
//!
//! Country flags used to need a MaxMind database: an account, a licence key, a
//! periodic download and a `GEOIP_DB_PATH` on every deployment. Its licence
//! also forbids shipping the database with the server, so flags could not be
//! distributable -- they were a setup step each operator had to remember, and
//! a deployment that forgot simply showed no flags.
//!
//! This table is built from the Regional Internet Registries' published
//! delegation records, which are open and redistributable, so it lives in the
//! repository and is linked into the binary by `include_bytes!`. Nothing to
//! install, and every deployment resolves the same address to the same country.
//!
//! The trade is accuracy: RIR records say which country a range was ALLOCATED
//! to, not where it is used today, so a multinational holder or a re-routed
//! block can report the registrant's country. For a flag beside a nickname
//! that is the right trade. An operator who needs better still sets
//! GEOIP_DB_PATH, and MaxMind takes precedence over this.
//!
//! Regenerate with `tools/gen_ip_country.py`.

use std::net::IpAddr;

static BLOB: &[u8] = include_bytes!("../data/ip_country.bin");

const MAGIC: &[u8; 8] = b"RCGEO1\0\0";
const HDR: usize = 8 + 4 + 4 + 2;
const V4_ENTRY: usize = 4 + 4 + 2;
const V6_ENTRY: usize = 8 + 8 + 2;

struct Table {
    codes: &'static [u8], // 2 bytes per country, index-addressed
    v4: &'static [u8],
    v6: &'static [u8],
    v4_count: usize,
    v6_count: usize,
}

fn u16le(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}
fn u32le(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}
fn u64le(b: &[u8], o: usize) -> u64 {
    let mut v = [0u8; 8];
    v.copy_from_slice(&b[o..o + 8]);
    u64::from_le_bytes(v)
}

/// Parse the header once. Returns None if the blob is malformed, which makes
/// the whole feature degrade to "no flags" rather than panicking a server.
fn table() -> Option<&'static Table> {
    use std::sync::OnceLock;
    static T: OnceLock<Option<Table>> = OnceLock::new();
    T.get_or_init(|| {
        if BLOB.len() < HDR || &BLOB[..8] != MAGIC {
            return None;
        }
        let v4_count = u32le(BLOB, 8) as usize;
        let v6_count = u32le(BLOB, 12) as usize;
        let cc_count = u16le(BLOB, 16) as usize;
        let codes_end = HDR + cc_count * 2;
        let v4_end = codes_end + v4_count * V4_ENTRY;
        let v6_end = v4_end + v6_count * V6_ENTRY;
        if BLOB.len() < v6_end {
            return None;
        }
        Some(Table {
            codes: &BLOB[HDR..codes_end],
            v4: &BLOB[codes_end..v4_end],
            v6: &BLOB[v4_end..v6_end],
            v4_count,
            v6_count,
        })
    })
    .as_ref()
}

impl Table {
    fn code(&self, idx: usize) -> Option<String> {
        let o = idx * 2;
        if o + 2 > self.codes.len() {
            return None;
        }
        std::str::from_utf8(&self.codes[o..o + 2])
            .ok()
            .map(|s| s.to_ascii_uppercase())
    }

    /// Last range whose start is <= key, then a containment check. The ranges
    /// are sorted and non-overlapping, so one binary search settles it.
    fn lookup_v4(&self, key: u32) -> Option<String> {
        let (mut lo, mut hi) = (0usize, self.v4_count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if u32le(self.v4, mid * V4_ENTRY) <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            return None;
        }
        let o = (lo - 1) * V4_ENTRY;
        // `end` is exclusive, as written by the generator.
        if key >= u32le(self.v4, o) && key < u32le(self.v4, o + 4) {
            self.code(u16le(self.v4, o + 8) as usize)
        } else {
            None
        }
    }

    /// IPv6 is matched on the top 64 bits: every RIR delegation is /64 or
    /// shorter, so the low half never distinguishes two entries.
    fn lookup_v6(&self, key_hi: u64) -> Option<String> {
        let (mut lo, mut hi) = (0usize, self.v6_count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if u64le(self.v6, mid * V6_ENTRY) <= key_hi {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            return None;
        }
        let o = (lo - 1) * V6_ENTRY;
        // `end` here is INCLUSIVE: a /0-style range would overflow an
        // exclusive end, and the generator writes (end - 1) >> 64 for that
        // reason.
        if key_hi >= u64le(self.v6, o) && key_hi <= u64le(self.v6, o + 8) {
            self.code(u16le(self.v6, o + 16) as usize)
        } else {
            None
        }
    }
}

/// ISO 3166-1 alpha-2 for `addr`, or None when the table has no answer.
/// Callers decide what a missing answer means; this never panics.
pub fn lookup(addr: IpAddr) -> Option<String> {
    let t = table()?;
    match addr {
        IpAddr::V4(v4) => t.lookup_v4(u32::from(v4)),
        IpAddr::V6(v6) => {
            // An IPv4-mapped or -compatible address is an IPv4 address wearing
            // a hat; answering from the v6 table would miss it entirely.
            if let Some(m) = v6.to_ipv4_mapped() {
                return t.lookup_v4(u32::from(m));
            }
            t.lookup_v6((u128::from(v6) >> 64) as u64)
        }
    }
}

/// Ranges in the built-in table, for a startup log.
pub fn range_counts() -> (usize, usize) {
    table().map(|t| (t.v4_count, t.v6_count)).unwrap_or((0, 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn the_embedded_table_parses() {
        let (v4, v6) = range_counts();
        assert!(v4 > 100_000, "ipv4 ranges: {v4}");
        assert!(v6 > 10_000, "ipv6 ranges: {v6}");
    }

    #[test]
    fn well_known_addresses_resolve() {
        // Google DNS (US), Cloudflare's 1.1.1.1 (APNIC research, AU), and a
        // RIPE-region address. Allocation-level answers, which is what this
        // table promises.
        assert_eq!(lookup(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))).as_deref(), Some("US"));
        assert!(lookup(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))).is_some());
        assert!(lookup(IpAddr::V4(Ipv4Addr::new(193, 0, 6, 139))).is_some());
    }

    #[test]
    fn codes_are_two_upper_case_letters() {
        for a in [
            Ipv4Addr::new(8, 8, 8, 8),
            Ipv4Addr::new(1, 1, 1, 1),
            Ipv4Addr::new(212, 58, 244, 22),
        ] {
            if let Some(cc) = lookup(IpAddr::V4(a)) {
                assert_eq!(cc.len(), 2, "{a} -> {cc}");
                assert!(cc.chars().all(|c| c.is_ascii_uppercase()), "{a} -> {cc}");
            }
        }
    }

    #[test]
    fn unallocated_and_reserved_space_has_no_answer() {
        // 0.0.0.0/8 and 240.0.0.0/4 are never delegated, so a hit here would
        // mean the search returned the neighbouring range instead of nothing.
        assert_eq!(lookup(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1))), None);
        assert_eq!(lookup(IpAddr::V4(Ipv4Addr::new(240, 0, 0, 1))), None);
        assert_eq!(lookup(IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255))), None);
    }

    #[test]
    fn an_ipv4_mapped_v6_address_answers_from_the_v4_table() {
        let mapped: Ipv6Addr = "::ffff:8.8.8.8".parse().unwrap();
        assert_eq!(lookup(IpAddr::V6(mapped)).as_deref(), Some("US"));
    }

    #[test]
    fn ipv6_resolves() {
        // Google public DNS64 / RIPE's own range.
        let g: Ipv6Addr = "2001:4860:4860::8888".parse().unwrap();
        assert!(lookup(IpAddr::V6(g)).is_some());
    }

    #[test]
    fn the_search_is_exact_at_range_edges() {
        // Walk the first entries and check the byte before a range start has a
        // different answer than the start itself. An off-by-one in the binary
        // search shows up here and nowhere else.
        let t = table().expect("table");
        for i in 0..64usize.min(t.v4_count) {
            let o = i * V4_ENTRY;
            let start = u32le(t.v4, o);
            let end = u32le(t.v4, o + 4);
            assert!(t.lookup_v4(start).is_some(), "start of range {i}");
            assert!(t.lookup_v4(end - 1).is_some(), "last address of range {i}");
            if start > 0 {
                // Either nothing, or a DIFFERENT range -- never this one by
                // accident.
                let before = t.lookup_v4(start - 1);
                let here = t.lookup_v4(start);
                if before.is_some() && before == here {
                    // Adjacent ranges of the same country are merged by the
                    // generator, so this must not happen.
                    panic!("range {i} was not merged with its neighbour");
                }
            }
        }
    }
}
