//! The PROXY protocol: who the client really is, when something sits in front.
//!
//! A load balancer or a TLS terminator opens the connection, so the address the
//! socket reports is the proxy's. HAProxy's PROXY protocol fixes that by
//! writing the original addresses as **the first bytes of the connection**,
//! before anything else - before a TLS handshake, and long before HTTP. So this
//! sits directly on the accepted socket: read the header, hand the rest of the
//! stream on untouched. If TLS is ever terminated here, it goes *after* this,
//! never before.
//!
//! Both versions are recognised by their first bytes, so one listener serves
//! proxied and direct connections without being told which to expect:
//!
//! * v1 is a line of text: `PROXY TCP4 198.51.100.7 10.0.0.2 56324 8081\r\n`
//! * v2 starts with the 12-byte signature `\r\n\r\n\0\r\nQUIT\n`
//!
//! **A header is only believed from a trusted peer.** Anyone can write those
//! bytes, so believing them from the open internet would let a client choose
//! what appears in the log - and what any address-based decision sees. The
//! default trust set is the private ranges and loopback: a proxy on the LAN is
//! believed, a client on the internet is not, and its bytes are left in the
//! stream to be parsed as the HTTP they claimed not to be (which fails, as it
//! should).

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

const V2_SIGNATURE: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";
const V1_PREFIX: &[u8; 6] = b"PROXY ";
/// v1 headers are at most 107 bytes plus CRLF.
const V1_MAX: usize = 108;

/// What to do with a connection's first bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// Never look; the peer address is the client address.
    Off,
    /// Read a header when a trusted peer sends one (the default).
    #[default]
    Auto,
    /// A trusted peer must send one; a connection without it is dropped.
    /// For a listener only the proxy can reach.
    Required,
}

impl Mode {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "off" | "none" | "false" => Ok(Mode::Off),
            "auto" => Ok(Mode::Auto),
            "required" => Ok(Mode::Required),
            other => Err(format!("proxy_protocol must be 'off', 'auto' or 'required', not '{other}'")),
        }
    }
}

/// The networks whose PROXY headers are believed.
#[derive(Debug, Clone, Default)]
pub struct Trusted(Vec<(IpAddr, u32)>);

/// Loopback, the private IPv4 ranges, link-local, and their IPv6 equivalents:
/// "something on our side of the network", which is where a proxy lives.
pub const DEFAULT_TRUSTED: &[&str] =
    &["127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "::1/128", "fc00::/7", "fe80::/10"];

impl Trusted {
    pub fn parse(cidrs: &[String]) -> Result<Self, String> {
        let mut out = Vec::new();
        for c in cidrs {
            let (addr, bits) = c.split_once('/').ok_or_else(|| format!("'{c}' is not a CIDR (address/prefix)"))?;
            let ip: IpAddr = addr.parse().map_err(|_| format!("'{c}': '{addr}' is not an address"))?;
            let bits: u32 = bits.parse().map_err(|_| format!("'{c}': '{bits}' is not a prefix length"))?;
            let max = if ip.is_ipv4() { 32 } else { 128 };
            if bits > max {
                return Err(format!("'{c}': /{bits} is too long for {}", if ip.is_ipv4() { "IPv4" } else { "IPv6" }));
            }
            out.push((ip, bits));
        }
        Ok(Self(out))
    }

    pub fn default_lan() -> Self {
        Self::parse(&DEFAULT_TRUSTED.iter().map(|s| s.to_string()).collect::<Vec<_>>()).expect("the defaults parse")
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        // An IPv4 address arriving on a dual-stack socket looks like ::ffff:a.b.c.d.
        let ip = match ip {
            IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(IpAddr::V6(v6)),
            v4 => v4,
        };
        self.0.iter().any(|(net, bits)| within(ip, *net, *bits))
    }
}

fn within(ip: IpAddr, net: IpAddr, bits: u32) -> bool {
    match (ip, net) {
        (IpAddr::V4(a), IpAddr::V4(n)) => {
            let (a, n) = (u32::from(a), u32::from(n));
            bits == 0 || (a ^ n) >> (32 - bits) == 0
        }
        (IpAddr::V6(a), IpAddr::V6(n)) => {
            let (a, n) = (u128::from(a), u128::from(n));
            bits == 0 || (a ^ n) >> (128 - bits) == 0
        }
        _ => false,
    }
}

/// A stream that replays bytes already read before it reads any more. What the
/// header parser did not consume belongs to whatever comes next - TLS, or HTTP.
pub struct Prefixed<S> {
    inner: S,
    prefix: Vec<u8>,
    at: usize,
}

impl<S> Prefixed<S> {
    pub fn new(inner: S, prefix: Vec<u8>) -> Self {
        Self { inner, prefix, at: 0 }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Prefixed<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if self.at < self.prefix.len() {
            let take = (self.prefix.len() - self.at).min(buf.remaining());
            let at = self.at;
            buf.put_slice(&self.prefix[at..at + take]);
            self.at += take;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Prefixed<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Read a PROXY header if there is one and it may be believed.
///
/// Returns the address to treat as the client's, and the stream positioned
/// where the header ended.
pub async fn accept<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    peer: SocketAddr,
    mode: Mode,
    trusted: &Trusted,
) -> io::Result<(Prefixed<S>, SocketAddr)> {
    if mode == Mode::Off {
        return Ok((Prefixed::new(stream, Vec::new()), peer));
    }
    let untrusted = !trusted.contains(peer.ip());
    if untrusted && mode == Mode::Required {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, format!("{peer} is not a trusted proxy")));
    }

    let mut stream = stream;
    let mut buf = Vec::with_capacity(V1_MAX);
    let mut probe = [0u8; 12];
    let mut have = 0;
    while have < probe.len() {
        let n = stream.read(&mut probe[have..]).await?;
        if n == 0 {
            break;
        }
        have += n;
    }
    buf.extend_from_slice(&probe[..have]);
    let looks_like_header = (have >= V2_SIGNATURE.len() && buf[..12] == *V2_SIGNATURE)
        || (have >= V1_PREFIX.len() && buf[..6] == *V1_PREFIX);

    if untrusted {
        // Looked, did not touch. The bytes go back into the stream and will be
        // read as the HTTP they claimed not to be - which fails, as it should.
        // Saying so here is the difference between a puzzling 400 and a
        // misconfigured `proxy_trust` an operator can fix.
        if looks_like_header {
            tracing::warn!(
                %peer,
                "PROXY header from an address that is not in proxy_trust: ignoring it, and the request will not parse"
            );
        }
        return Ok((Prefixed::new(stream, buf), peer));
    }

    if have >= V2_SIGNATURE.len() && buf[..12] == *V2_SIGNATURE {
        return read_v2(stream, buf, peer).await;
    }
    if have >= V1_PREFIX.len() && buf[..6] == *V1_PREFIX {
        return read_v1(stream, buf, peer).await;
    }
    if mode == Mode::Required {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("{peer} sent no PROXY header")));
    }
    Ok((Prefixed::new(stream, buf), peer))
}

async fn read_v1<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    mut buf: Vec<u8>,
    peer: SocketAddr,
) -> io::Result<(Prefixed<S>, SocketAddr)> {
    // Up to and including CRLF, and no further: the next byte is the payload's.
    while !buf.windows(2).any(|w| w == b"\r\n") {
        if buf.len() > V1_MAX {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "PROXY v1 header too long"));
        }
        let mut b = [0u8; 1];
        if stream.read(&mut b).await? == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "PROXY v1 header cut short"));
        }
        buf.push(b[0]);
    }
    let end = buf.windows(2).position(|w| w == b"\r\n").expect("just checked") + 2;
    let line = String::from_utf8_lossy(&buf[..end - 2]).into_owned();
    let rest = buf[end..].to_vec();
    let client = parse_v1(&line).unwrap_or(peer);
    Ok((Prefixed::new(stream, rest), client))
}

/// `PROXY TCP4 <src> <dst> <sport> <dport>`; `UNKNOWN` means the proxy could
/// not tell, and then the peer is as good as it gets.
fn parse_v1(line: &str) -> Option<SocketAddr> {
    let mut parts = line.split_ascii_whitespace();
    if parts.next()? != "PROXY" {
        return None;
    }
    match parts.next()? {
        "TCP4" | "TCP6" => {}
        _ => return None,
    }
    let src: IpAddr = parts.next()?.parse().ok()?;
    let _dst = parts.next()?;
    let port: u16 = parts.next()?.parse().ok()?;
    Some(SocketAddr::new(src, port))
}

async fn read_v2<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    mut buf: Vec<u8>,
    peer: SocketAddr,
) -> io::Result<(Prefixed<S>, SocketAddr)> {
    while buf.len() < 16 {
        let mut b = [0u8; 16];
        let n = stream.read(&mut b[..16 - buf.len()]).await?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "PROXY v2 header cut short"));
        }
        buf.extend_from_slice(&b[..n]);
    }
    let ver_cmd = buf[12];
    let family = buf[13];
    let len = u16::from_be_bytes([buf[14], buf[15]]) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;

    // High nibble 2 is this version; command 1 is PROXY, 0 is LOCAL (a health
    // check from the proxy itself, which speaks for nobody).
    let client = if ver_cmd >> 4 == 2 && ver_cmd & 0x0f == 1 {
        match (family >> 4, body.len()) {
            // AF_INET, TCP or UDP
            (1, n) if n >= 12 => {
                let src = std::net::Ipv4Addr::new(body[0], body[1], body[2], body[3]);
                Some(SocketAddr::new(IpAddr::V4(src), u16::from_be_bytes([body[8], body[9]])))
            }
            (2, n) if n >= 36 => {
                let mut o = [0u8; 16];
                o.copy_from_slice(&body[..16]);
                Some(SocketAddr::new(IpAddr::V6(o.into()), u16::from_be_bytes([body[32], body[33]])))
            }
            // AF_UNSPEC / AF_UNIX: nothing to learn.
            _ => None,
        }
    } else {
        None
    };
    Ok((Prefixed::new(stream, Vec::new()), client.unwrap_or(peer)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn addr(s: &str) -> SocketAddr {
        s.parse().expect("address")
    }

    #[test]
    fn only_our_side_of_the_network_is_believed() {
        let t = Trusted::default_lan();
        assert!(t.contains("127.0.0.1".parse().unwrap()));
        assert!(t.contains("10.4.5.6".parse().unwrap()));
        assert!(t.contains("192.168.44.99".parse().unwrap()));
        assert!(t.contains("172.16.0.1".parse().unwrap()));
        assert!(t.contains("::1".parse().unwrap()));
        // A v4 client on a dual-stack socket arrives mapped.
        assert!(t.contains("::ffff:10.0.0.9".parse().unwrap()));
        // The open internet is not.
        assert!(!t.contains("198.51.100.7".parse().unwrap()));
        assert!(!t.contains("8.8.8.8".parse().unwrap()));
        assert!(!t.contains("172.32.0.1".parse().unwrap()), "just outside 172.16/12");
        assert!(!t.contains("2001:db8::1".parse().unwrap()));
    }

    #[test]
    fn a_cidr_that_makes_no_sense_is_refused() {
        assert!(Trusted::parse(&["10.0.0.0".into()]).is_err(), "no prefix");
        assert!(Trusted::parse(&["10.0.0.0/33".into()]).is_err(), "too long for v4");
        assert!(Trusted::parse(&["nonsense/8".into()]).is_err());
        assert!(Trusted::parse(&["10.0.0.0/8".into(), "::1/128".into()]).is_ok());
    }

    #[tokio::test]
    async fn a_v1_header_from_the_lan_names_the_client() {
        let stream = std::io::Cursor::new(b"PROXY TCP4 198.51.100.7 10.0.0.2 56324 8081\r\nGET / HTTP/1.1\r\n".to_vec());
        let (mut rest, client) = accept(stream, addr("10.0.0.2:40000"), Mode::Auto, &Trusted::default_lan()).await.unwrap();
        assert_eq!(client, addr("198.51.100.7:56324"));
        // And the payload starts exactly after the header.
        let mut out = String::new();
        rest.read_to_string(&mut out).await.unwrap();
        assert_eq!(out, "GET / HTTP/1.1\r\n");
    }

    #[tokio::test]
    async fn a_v2_header_from_the_lan_names_the_client() {
        let mut h = V2_SIGNATURE.to_vec();
        h.push(0x21); // version 2, PROXY
        h.push(0x11); // AF_INET, STREAM
        h.extend_from_slice(&12u16.to_be_bytes());
        h.extend_from_slice(&[198, 51, 100, 7]); // source
        h.extend_from_slice(&[10, 0, 0, 2]); // destination
        h.extend_from_slice(&4711u16.to_be_bytes()); // source port
        h.extend_from_slice(&8081u16.to_be_bytes());
        h.extend_from_slice(b"hello");
        let (mut rest, client) =
            accept(std::io::Cursor::new(h), addr("10.0.0.2:40000"), Mode::Auto, &Trusted::default_lan()).await.unwrap();
        assert_eq!(client, addr("198.51.100.7:4711"));
        let mut out = String::new();
        rest.read_to_string(&mut out).await.unwrap();
        assert_eq!(out, "hello");
    }

    #[tokio::test]
    async fn a_header_from_the_internet_is_not_believed() {
        // The bytes stay in the stream: they were never a header, they were a
        // client pretending. Whatever parses next will reject them.
        let claim = b"PROXY TCP4 10.0.0.1 10.0.0.2 1 2\r\nGET / HTTP/1.1\r\n";
        let peer = addr("198.51.100.7:33000");
        let (mut rest, client) =
            accept(std::io::Cursor::new(claim.to_vec()), peer, Mode::Auto, &Trusted::default_lan()).await.unwrap();
        assert_eq!(client, peer, "the socket is the only thing worth believing here");
        let mut out = Vec::new();
        rest.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, claim, "not one byte was eaten");
    }

    #[tokio::test]
    async fn a_direct_connection_is_left_alone() {
        let payload = b"GET /subjects HTTP/1.1\r\n";
        let (mut rest, client) = accept(
            std::io::Cursor::new(payload.to_vec()),
            addr("10.0.0.5:5000"),
            Mode::Auto,
            &Trusted::default_lan(),
        )
        .await
        .unwrap();
        assert_eq!(client, addr("10.0.0.5:5000"));
        let mut out = Vec::new();
        rest.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, payload, "the probe put back everything it read");
    }

    #[tokio::test]
    async fn required_means_required() {
        let direct = std::io::Cursor::new(b"GET / HTTP/1.1\r\n".to_vec());
        assert!(accept(direct, addr("10.0.0.5:5000"), Mode::Required, &Trusted::default_lan()).await.is_err());
        // ...and an untrusted peer cannot satisfy it by claiming one.
        let claim = std::io::Cursor::new(b"PROXY TCP4 10.0.0.1 10.0.0.2 1 2\r\n".to_vec());
        assert!(accept(claim, addr("198.51.100.7:1"), Mode::Required, &Trusted::default_lan()).await.is_err());
    }

    #[tokio::test]
    async fn a_local_health_check_speaks_for_nobody() {
        // v2 LOCAL: the proxy checking on us, not forwarding anyone.
        let mut h = V2_SIGNATURE.to_vec();
        h.push(0x20); // version 2, LOCAL
        h.push(0x00); // AF_UNSPEC
        h.extend_from_slice(&0u16.to_be_bytes());
        let peer = addr("10.0.0.2:40000");
        let (_, client) = accept(std::io::Cursor::new(h), peer, Mode::Auto, &Trusted::default_lan()).await.unwrap();
        assert_eq!(client, peer);
    }
}
