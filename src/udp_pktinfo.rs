//! Linux UDP source pinning via `IP_PKTINFO`.
//!
//! Dual-homed hosts (e.g. ethernet + WLAN on the same subnet) otherwise pick
//! the wrong outbound source on `send_to`, and MotK drops the datagram because
//! `rnet_transport_recv` requires `src == dialed peer`.

use std::io::{self, IoSlice, IoSliceMut};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::fd::AsRawFd;

use nix::cmsg_space;
use nix::sys::socket::sockopt::Ipv4PacketInfo;
use nix::sys::socket::{
    recvmsg, sendmsg, ControlMessage, ControlMessageOwned, MsgFlags, SockaddrIn, SockaddrStorage,
};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

/// Bind `0.0.0.0:port` (or the given bind string), enable `IP_PKTINFO`, wrap in Tokio.
pub fn bind_udp_with_pktinfo(bind: &str) -> io::Result<UdpSocket> {
    let addr: SocketAddr = bind
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    if addr.is_ipv4() {
        nix::sys::socket::setsockopt(&sock, Ipv4PacketInfo, &true)?;
    }
    let std_sock: std::net::UdpSocket = sock.into();
    UdpSocket::from_std(std_sock)
}

#[derive(Debug, Clone, Copy)]
pub struct RecvSas {
    pub n: usize,
    pub peer: SocketAddr,
    /// Local IPv4 the peer addressed (header destination / pktinfo).
    pub local_dst: Option<Ipv4Addr>,
}

/// Non-blocking `recvmsg` with `IP_PKTINFO`. Call after `readable()`.
pub fn try_recv_sas(sock: &UdpSocket, buf: &mut [u8]) -> io::Result<RecvSas> {
    let mut iov = [IoSliceMut::new(buf)];
    let mut cmsg = cmsg_space!(libc::in_pktinfo);
    let msg = recvmsg::<SockaddrStorage>(
        sock.as_raw_fd(),
        &mut iov,
        Some(&mut cmsg),
        MsgFlags::empty(),
    )
    .map_err(nix_to_io)?;

    let n = msg.bytes;
    let peer = msg
        .address
        .as_ref()
        .and_then(storage_to_socket_addr)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "recvmsg missing peer"))?;

    let mut local_dst = None;
    for c in msg.cmsgs().map_err(nix_to_io)? {
        if let ControlMessageOwned::Ipv4PacketInfo(info) = c {
            // Destination address from the IP header (what the peer dialed).
            local_dst = Some(Ipv4Addr::from(u32::from_be(info.ipi_addr.s_addr)));
            break;
        }
    }
    Ok(RecvSas { n, peer, local_dst })
}

/// Non-blocking `sendmsg` pinning IPv4 source. Call after `writable()` when needed.
pub fn try_send_sas(
    sock: &UdpSocket,
    buf: &[u8],
    peer: SocketAddr,
    source: Ipv4Addr,
) -> io::Result<usize> {
    let iov = [IoSlice::new(buf)];
    let sockaddr = match peer {
        SocketAddr::V4(v4) => SockaddrIn::from(v4),
        SocketAddr::V6(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPv6 send_sas not implemented",
            ));
        }
    };
    let pktinfo = libc::in_pktinfo {
        ipi_ifindex: 0,
        ipi_spec_dst: libc::in_addr {
            s_addr: u32::from(source).to_be(),
        },
        ipi_addr: libc::in_addr { s_addr: 0 },
    };
    let cmsgs = [ControlMessage::Ipv4PacketInfo(&pktinfo)];
    sendmsg(
        sock.as_raw_fd(),
        &iov,
        &cmsgs,
        MsgFlags::empty(),
        Some(&sockaddr),
    )
    .map_err(nix_to_io)
}

fn storage_to_socket_addr(ss: &SockaddrStorage) -> Option<SocketAddr> {
    ss.as_sockaddr_in()
        .map(|v4| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(v4.ip()), v4.port())))
        .or_else(|| {
            ss.as_sockaddr_in6()
                .map(|v6| SocketAddr::V6(std::net::SocketAddrV6::new(v6.ip(), v6.port(), 0, 0)))
        })
}

fn nix_to_io(err: nix::Error) -> io::Error {
    io::Error::from(err)
}

/// Parse a host string as IPv4 when possible (advertise / LAN fallback).
pub fn parse_ipv4_host(host: &str) -> Option<Ipv4Addr> {
    let h = host.trim();
    if h.is_empty() {
        return None;
    }
    h.parse::<Ipv4Addr>()
        .ok()
        .or_else(|| match h.parse::<IpAddr>() {
            Ok(IpAddr::V4(v4)) => Some(v4),
            _ => None,
        })
}
