#!/usr/bin/env python3
"""Build the built-in IP -> country table from the RIR delegation files.

Why these files, and not MaxMind: GeoLite2 needs an account, a licence key and
a periodic download, and its licence does not let us redistribute the database
with the server. That makes country flags a per-deployment setup step that
every operator gets to forget -- which is exactly what happened. The Regional
Internet Registries publish their allocation records openly and allow
redistribution, so the table they produce can be committed here and compiled
into the binary. Every deployment then behaves identically with nothing to
install.

The trade is accuracy: this is the country a range was ALLOCATED to, not where
the address is used today, so a multinational holder or a re-routed block can
show the registrant's country. For a flag beside a nickname that is fine, and
an operator who needs better can still point GEOIP_DB_PATH at MaxMind, which
takes precedence.

Usage:
    python3 tools/gen_ip_country.py              # fetch and write data/ip_country.bin
    python3 tools/gen_ip_country.py --from DIR   # use already-downloaded files

Re-run when the table starts to feel stale; quarterly is plenty. The output is
deterministic for a given input, so an unchanged internet means an unchanged
file and no commit.
"""
import argparse
import glob
import ipaddress
import os
import struct
import sys
import urllib.request

RIRS = {
    "afrinic": "https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest",
    "apnic":   "https://ftp.apnic.net/stats/apnic/delegated-apnic-extended-latest",
    "arin":    "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest",
    "lacnic":  "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest",
    "ripencc": "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest",
}

MAGIC = b"RCGEO1\0\0"


def fetch(dest_dir):
    os.makedirs(dest_dir, exist_ok=True)
    for name, url in RIRS.items():
        path = os.path.join(dest_dir, name + ".txt")
        sys.stderr.write("fetching %s\n" % name)
        with urllib.request.urlopen(url, timeout=180) as r, open(path, "wb") as f:
            f.write(r.read())
    return dest_dir


def parse(src_dir):
    v4, v6 = [], []
    files = sorted(glob.glob(os.path.join(src_dir, "*.txt")))
    if not files:
        sys.exit("no RIR files in %s" % src_dir)
    for path in files:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                p = line.rstrip("\n").split("|")
                # registry|cc|type|start|value|date|status|...
                if len(p) < 7 or p[1] in ("*", ""):
                    continue
                # Only ranges actually handed out. "reserved" and "available"
                # carry no country and would otherwise map to a bogus flag.
                if p[6] not in ("allocated", "assigned"):
                    continue
                cc = p[1].upper()
                if len(cc) != 2 or not cc.isalpha():
                    continue
                if p[2] == "ipv4":
                    try:
                        start = int(ipaddress.IPv4Address(p[3]))
                        count = int(p[4])
                    except ValueError:
                        continue
                    # RIR ipv4 "value" is a host count, not a prefix length,
                    # and is not always a power of two.
                    v4.append((start, start + count, cc))
                elif p[2] == "ipv6":
                    try:
                        net = ipaddress.IPv6Network("%s/%s" % (p[3], p[4]), strict=False)
                    except ValueError:
                        continue
                    v6.append((int(net.network_address),
                               int(net.broadcast_address) + 1, cc))
    return v4, v6


def merge(ranges):
    """Sort and coalesce touching ranges that share a country."""
    ranges.sort()
    out = []
    for start, end, cc in ranges:
        if out and out[-1][2] == cc and out[-1][1] >= start:
            out[-1] = (out[-1][0], max(out[-1][1], end), cc)
        else:
            out.append((start, end, cc))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="src", help="directory of already-downloaded RIR files")
    ap.add_argument("-o", dest="out", default=None, help="output .bin")
    args = ap.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out = args.out or os.path.join(root, "data", "ip_country.bin")
    src = args.src or fetch(os.path.join(root, "data", "rir"))

    v4, v6 = parse(src)
    v4, v6 = merge(v4), merge(v6)

    codes = sorted({cc for _, _, cc in v4} | {cc for _, _, cc in v6})
    if len(codes) > 0xFFFF:
        sys.exit("too many country codes")
    idx = {cc: i for i, cc in enumerate(codes)}

    body = bytearray()
    body += MAGIC
    body += struct.pack("<IIH", len(v4), len(v6), len(codes))
    for cc in codes:
        body += cc.encode("ascii")
    for start, end, cc in v4:
        body += struct.pack("<IIH", start, end, idx[cc])
    for start, end, cc in v6:
        # Top 64 bits only. Every RIR IPv6 delegation is /64 or shorter, so the
        # low half is always zero and storing it would double the table to say
        # nothing.
        body += struct.pack("<QQH", start >> 64, (end - 1) >> 64, idx[cc])

    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "wb") as f:
        f.write(body)
    sys.stderr.write(
        "wrote %s: %d ipv4 ranges, %d ipv6 ranges, %d countries, %.2f MB\n"
        % (out, len(v4), len(v6), len(codes), len(body) / 1024 / 1024))


if __name__ == "__main__":
    main()
