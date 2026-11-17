# Byte-level writers for the vendor formats, shared by the reader tests and
# the benchmarks: Nasdaq TotalView-ITCH 5.0 messages with their BinaryFILE
# length prefix (`ITCHBytes`), and IEX-TP segments in Ethernet/IPv4/UDP frames
# inside pcapng or classic pcap records (`IEXBytes`). Every writer returns a
# `Vector{UInt8}`.

module ITCHBytes

be(x, n) = UInt8[(UInt64(x) >> (8 * (n - k))) & 0xff for k in 1:n]
alpha(s, n) = UInt8.(collect(rpad(s, n)))
msg(type, locate, ts, body) = (
    m = vcat(UInt8(type), be(locate, 2), be(0, 2), be(ts, 6), body);
    vcat(be(length(m), 2), m)
)
R(loc, stock, cat, lot) = msg(
    'R',
    loc,
    0,
    vcat(
        alpha(stock, 8),
        UInt8(cat),
        UInt8(' '),
        be(lot, 4),
        UInt8('N'),
        UInt8(' '),
        alpha("", 2),
        UInt8('P'),
        alpha("", 4),
        be(0, 4),
        UInt8('N'),
    ),
)
A(loc, ts, ref, side, sh, stock, px) =
    msg('A', loc, ts, vcat(be(ref, 8), UInt8(side), be(sh, 4), alpha(stock, 8), be(px, 4)))
F(loc, ts, ref, side, sh, stock, px) = msg(
    'F',
    loc,
    ts,
    vcat(be(ref, 8), UInt8(side), be(sh, 4), alpha(stock, 8), be(px, 4), alpha("NSDQ", 4)),
)
E(loc, ts, ref, sh, match) = msg('E', loc, ts, vcat(be(ref, 8), be(sh, 4), be(match, 8)))
C(loc, ts, ref, sh, match, printable, px) = msg(
    'C',
    loc,
    ts,
    vcat(be(ref, 8), be(sh, 4), be(match, 8), UInt8(printable), be(px, 4)),
)
X(loc, ts, ref, sh) = msg('X', loc, ts, vcat(be(ref, 8), be(sh, 4)))
D(loc, ts, ref) = msg('D', loc, ts, be(ref, 8))
U(loc, ts, orig, new, sh, px) =
    msg('U', loc, ts, vcat(be(orig, 8), be(new, 8), be(sh, 4), be(px, 4)))
P(loc, ts, sh, stock, px, match) = msg(
    'P',
    loc,
    ts,
    vcat(be(0, 8), UInt8('B'), be(sh, 4), alpha(stock, 8), be(px, 4), be(match, 8)),
)
Q(loc, ts, sh, stock, px, match, cross) = msg(
    'Q',
    loc,
    ts,
    vcat(be(sh, 8), alpha(stock, 8), be(px, 4), be(match, 8), UInt8(cross)),
)
B(loc, ts, match) = msg('B', loc, ts, be(match, 8))
S(event) = msg('S', 0, 0, [UInt8(event)])
H(loc) = msg('H', loc, 0, zeros(UInt8, 14))                    # skipped by length

end

module IEXBytes

le(x, n) = (
    u = x < 0 ? reinterpret(UInt64, Int64(x)) : UInt64(x);
    UInt8[(u >> (8 * (k - 1))) & 0xff for k in 1:n]
)
be(x, n) = UInt8[(UInt64(x) >> (8 * (n - k))) & 0xff for k in 1:n]
sym(s) = UInt8.(collect(rpad(s, 8)))
S(ev, ts) = vcat(UInt8('S'), UInt8(ev), le(ts, 8))
Qm(s, ts) = vcat(
    UInt8('Q'),
    0x00,
    le(ts, 8),
    sym(s),
    le(100, 4),
    le(1_000_000, 8),
    le(1_000_100, 8),
    le(100, 4),
)
T(s, flags, ts, size, px, id; reserved = 0) = vcat(
    UInt8('T'),
    UInt8(flags),
    le(ts, 8),
    sym(s),
    le(size, 4),
    le(px, 8),
    le(id, 8),
    zeros(UInt8, reserved),
)
B(s, ts, id) = vcat(UInt8('B'), 0x00, le(ts, 8), sym(s), le(1, 4), le(0, 8), le(id, 8))
function segment(proto, session, seq, msgs; send = 0)
    payload =
        isempty(msgs) ? UInt8[] : reduce(vcat, [vcat(le(length(m), 2), m) for m in msgs])
    return vcat(
        0x01,
        0x00,
        le(proto, 2),
        le(1, 4),
        le(session, 4),
        le(length(payload), 2),
        le(length(msgs), 2),
        le(0, 8),
        le(seq, 8),
        le(send, 8),
        payload,
    )
end
function frame(seg; vlan = false)
    udp = vcat(be(10377, 2), be(10377, 2), be(8 + length(seg), 2), be(0, 2), seg)
    ip = vcat(
        0x45,
        0x00,
        be(20 + length(udp), 2),
        be(0, 2),
        be(0, 2),
        0x40,
        0x11,
        be(0, 2),
        UInt8[23, 226, 155, 131],
        UInt8[233, 215, 21, 3],
        udp,
    )
    eth = vcat(
        UInt8[0x01, 0x00, 0x5e, 0x57, 0x15, 0x03],
        UInt8[0, 0x11, 0x22, 0x33, 0x44, 0x55],
    )
    return vlan ? vcat(eth, be(0x8100, 2), be(1017, 2), be(0x0800, 2), ip) :
           vcat(eth, be(0x0800, 2), ip)
end
pad4(v) = vcat(v, zeros(UInt8, (4 - length(v) % 4) % 4))
shb() = vcat(
    le(0x0A0D0D0A, 4),
    le(28, 4),
    le(0x1A2B3C4D, 4),
    le(1, 2),
    le(0, 2),
    le(-1, 8),
    le(28, 4),
)
idb() = vcat(le(1, 4), le(20, 4), le(1, 2), le(0, 2), le(65535, 4), le(20, 4))
function epb(data)
    body = vcat(
        le(0, 4),
        le(0, 4),
        le(0, 4),
        le(length(data), 4),
        le(length(data), 4),
        pad4(data),
    )
    return vcat(le(6, 4), le(8 + length(body) + 4, 4), body, le(8 + length(body) + 4, 4))
end
pcap_header() =
    vcat(le(0xa1b2c3d4, 4), le(2, 2), le(4, 2), le(0, 4), le(0, 4), le(65535, 4), le(1, 4))
pcap_record(data) = vcat(le(0, 4), le(0, 4), le(length(data), 4), le(length(data), 4), data)

end
