//! Discover this host's public IPv4 via RFC 5389 STUN Binding.
//!
//! Used when `INPUT_RELAY_ADVERTISE_HOST` / `PUBLIC_HOST` are unset so the UDP
//! input relay never advertises `127.0.0.1` to remote peers.

use anyhow::{bail, Context, Result};
use rand::RngCore;
use std::net::{ToSocketAddrs, UdpSocket};
use std::time::Duration;

const STUN_MAGIC: u32 = 0x2112_A442;
const STUN_BINDING_REQUEST: u16 = 0x0001;
const STUN_BINDING_SUCCESS: u16 = 0x0101;
const STUN_ATTR_MAPPED_ADDRESS: u16 = 0x0001;
const STUN_ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;
const STUN_HEADER: usize = 20;
const DEFAULT_STUN: &str = "stun.l.google.com:19302";

fn is_loopback_host(host: &str) -> bool {
    let h = host.trim().to_ascii_lowercase();
    h.is_empty() || h == "127.0.0.1" || h == "::1" || h == "localhost" || h.starts_with("127.")
}

/// True when `host` is empty, loopback, or otherwise unsafe to advertise WAN peers.
pub fn advertise_host_needs_public(host: &str) -> bool {
    is_loopback_host(host)
}

/// Resolve STUN server host:port list from env, then defaults.
pub fn stun_endpoints_from_env() -> Vec<String> {
    let mut out = Vec::new();
    for key in ["INPUT_RELAY_STUN", "COTURN_STUN_HOST", "COTURN_HOST"] {
        if let Ok(raw) = std::env::var(key) {
            let t = raw.trim();
            if t.is_empty() {
                continue;
            }
            if t.contains(':') {
                out.push(t.to_string());
            } else {
                let port = std::env::var("COTURN_STUN_PORT")
                    .ok()
                    .and_then(|p| p.parse::<u16>().ok())
                    .unwrap_or(3478);
                out.push(format!("{t}:{port}"));
            }
        }
    }
    out.push(DEFAULT_STUN.to_string());
    out
}

/// STUN Binding → XOR-MAPPED / MAPPED IPv4. Tries each endpoint until one works.
pub fn discover_ipv4(timeout: Duration) -> Result<String> {
    let mut last_err = None;
    for ep in stun_endpoints_from_env() {
        match discover_ipv4_via(&ep, timeout) {
            Ok(ip) => return Ok(ip),
            Err(e) => {
                tracing::debug!(stun = %ep, error = %e, "STUN public-IP probe failed");
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no STUN endpoints"))).context(
        "failed to discover public IPv4 via STUN (set PUBLIC_HOST or INPUT_RELAY_ADVERTISE_HOST)",
    )
}

fn discover_ipv4_via(endpoint: &str, timeout: Duration) -> Result<String> {
    let addr = endpoint
        .to_socket_addrs()
        .with_context(|| format!("resolve STUN {endpoint}"))?
        .find(|a| a.is_ipv4())
        .with_context(|| format!("no IPv4 for STUN {endpoint}"))?;

    let sock = UdpSocket::bind("0.0.0.0:0").context("STUN UDP bind")?;
    sock.set_read_timeout(Some(timeout))
        .context("STUN set_read_timeout")?;
    sock.set_write_timeout(Some(timeout))
        .context("STUN set_write_timeout")?;

    let mut txid = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut txid);

    let mut req = [0u8; STUN_HEADER];
    req[0..2].copy_from_slice(&STUN_BINDING_REQUEST.to_be_bytes());
    req[2..4].copy_from_slice(&0u16.to_be_bytes()); // length
    req[4..8].copy_from_slice(&STUN_MAGIC.to_be_bytes());
    req[8..20].copy_from_slice(&txid);

    sock.send_to(&req, addr)
        .with_context(|| format!("STUN send to {addr}"))?;

    let mut buf = [0u8; 512];
    let (n, _) = sock
        .recv_from(&mut buf)
        .with_context(|| format!("STUN recv from {addr}"))?;
    parse_binding_ipv4(&buf[..n], &txid)
}

fn read_u16(b: &[u8]) -> u16 {
    u16::from_be_bytes([b[0], b[1]])
}

fn read_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn parse_binding_ipv4(packet: &[u8], txid: &[u8; 12]) -> Result<String> {
    if packet.len() < STUN_HEADER {
        bail!("STUN response too short");
    }
    if read_u16(&packet[0..2]) != STUN_BINDING_SUCCESS {
        bail!("STUN not Binding success");
    }
    if read_u32(&packet[4..8]) != STUN_MAGIC {
        bail!("STUN bad magic cookie");
    }
    if &packet[8..20] != txid {
        bail!("STUN transaction id mismatch");
    }
    let msg_len = read_u16(&packet[2..4]) as usize;
    if msg_len + STUN_HEADER != packet.len() || (msg_len & 3) != 0 {
        bail!("STUN bad message length");
    }

    let mut mapped: Option<(u32, u16)> = None;
    let mut xor_mapped: Option<(u32, u16)> = None;
    let mut off = STUN_HEADER;
    while off + 4 <= packet.len() {
        let atype = read_u16(&packet[off..off + 2]);
        let alen = read_u16(&packet[off + 2..off + 4]) as usize;
        let val_off = off + 4;
        let val_end = val_off + alen;
        if val_end > packet.len() {
            bail!("STUN attribute overrun");
        }
        let val = &packet[val_off..val_end];
        if (atype == STUN_ATTR_MAPPED_ADDRESS || atype == STUN_ATTR_XOR_MAPPED_ADDRESS)
            && alen >= 8
            && val[1] == 0x01
        {
            // family IPv4 at val[1]; port at [2..4]; addr at [4..8]
            let mut port = read_u16(&val[2..4]);
            let mut addr = read_u32(&val[4..8]);
            if atype == STUN_ATTR_XOR_MAPPED_ADDRESS {
                port ^= (STUN_MAGIC >> 16) as u16;
                addr ^= STUN_MAGIC;
            }
            if ipv4_usable(addr) {
                if atype == STUN_ATTR_XOR_MAPPED_ADDRESS {
                    xor_mapped = Some((addr, port));
                } else {
                    mapped = Some((addr, port));
                }
            }
        }
        // attributes padded to 4 bytes
        off = val_end + ((4 - (alen % 4)) % 4);
    }

    let (addr, _) = xor_mapped
        .or(mapped)
        .context("STUN response missing mapped IPv4")?;
    Ok(format!(
        "{}.{}.{}.{}",
        (addr >> 24) & 0xff,
        (addr >> 16) & 0xff,
        (addr >> 8) & 0xff,
        addr & 0xff
    ))
}

fn ipv4_usable(addr: u32) -> bool {
    let first = addr >> 24;
    first != 0 && first < 224 && addr != 0xffff_ffff
}

#[allow(dead_code)]
pub fn format_socket(ip: &str, port: u16) -> String {
    format!("{ip}:{port}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_detected() {
        assert!(advertise_host_needs_public(""));
        assert!(advertise_host_needs_public("127.0.0.1"));
        assert!(advertise_host_needs_public("localhost"));
        assert!(!advertise_host_needs_public("203.0.113.1"));
        assert!(!advertise_host_needs_public("netplay.example.com"));
    }

    #[test]
    fn parse_xor_mapped() {
        // Minimal Binding success with XOR-MAPPED-ADDRESS for 203.0.113.50:3478
        let mut pkt = vec![0u8; STUN_HEADER + 12];
        pkt[0..2].copy_from_slice(&STUN_BINDING_SUCCESS.to_be_bytes());
        pkt[2..4].copy_from_slice(&12u16.to_be_bytes());
        pkt[4..8].copy_from_slice(&STUN_MAGIC.to_be_bytes());
        let txid = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        pkt[8..20].copy_from_slice(&txid);
        pkt[20..22].copy_from_slice(&STUN_ATTR_XOR_MAPPED_ADDRESS.to_be_bytes());
        pkt[22..24].copy_from_slice(&8u16.to_be_bytes());
        pkt[24] = 0;
        pkt[25] = 0x01; // IPv4
        let port = 3478u16 ^ ((STUN_MAGIC >> 16) as u16);
        let addr = u32::from_be_bytes([203, 0, 113, 50]) ^ STUN_MAGIC;
        pkt[26..28].copy_from_slice(&port.to_be_bytes());
        pkt[28..32].copy_from_slice(&addr.to_be_bytes());
        let ip = parse_binding_ipv4(&pkt, &txid).unwrap();
        assert_eq!(ip, "203.0.113.50");
    }
}
