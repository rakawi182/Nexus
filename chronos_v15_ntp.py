#!/usr/bin/env python3
"""
CHRONOS-NEXUS NTP companion (stdlib only; works in Termux / Pydroid / desktop Python).

Queries several stratum-1/2 NTP servers over UDP (which a browser cannot do),
fuses them, and serves the result at http://127.0.0.1:8123/time as JSON:
    {"ms": <corrected UTC epoch ms>, "u": <uncertainty ms>, "note": "...", "servers": [...]}
The PWA reads it as the 'Local NTP' source and compensates the loopback round trip itself.

Usage:  python chronos_ntp.py                 # run the service
        python chronos_ntp.py --once          # one measurement, print, exit
        python chronos_ntp.py --servers tick.usno.navy.mil,time.nist.gov,pool.ntp.org
"""
import argparse, json, socket, struct, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

D = 2208988800  # seconds between 1900-01-01 and 1970-01-01
# Near/regional servers give the best precision; the national-lab servers are far but
# independent, so they act as cross-checks. Servers that do not answer are simply skipped.
DEFAULT = [
    "0.id.pool.ntp.org", "1.id.pool.ntp.org", "2.id.pool.ntp.org",
    "0.asia.pool.ntp.org", "1.asia.pool.ntp.org", "sg.pool.ntp.org",
    "ntp.nict.jp", "time.google.com", "time.cloudflare.com", "time.apple.com", "time.windows.com",
    "tick.usno.navy.mil", "tock.usno.navy.mil", "time.nist.gov", "ptbtime1.ptb.de",
]
# Polite sampling for primary (stratum-1) lab servers: (max samples, min gap in s).
# NIST asks clients not to query more often than once every 4 s.
LABS = {"usno.navy.mil": (3, 2.0), "nist.gov": (2, 4.1), "ptb.de": (3, 2.0), "nict.jp": (3, 2.0)}
BACKOFF = {}  # host -> time.time() until which we stay silent after a Kiss-o'-Death


class KoD(Exception):
    pass


def to_ntp(ns):
    s, r = divmod(ns, 10**9)
    return (s + D) & 0xFFFFFFFF, (r << 32) // 10**9


def from_ntp(s, f):
    return (s - D) * 10**9 + ((f * 10**9) >> 32)


def resolve(spec):
    host, _, port = spec.partition(":")
    return socket.getaddrinfo(host, int(port or 123), socket.AF_INET, socket.SOCK_DGRAM)[0][4]


def query(addr, timeout=2.0):
    """One NTP exchange. Returns (offset_ms, delay_ms, stratum); offset = server - local clock."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as so:
        so.settimeout(timeout)
        t1 = time.time_ns(); p1 = time.perf_counter_ns()
        sec, frac = to_ntp(t1)
        pkt = struct.pack('!BBBb11I', 0x23, 0, 0, 0, *([0] * 9), sec, frac)  # v4, client
        so.sendto(pkt, addr)
        data, _ = so.recvfrom(512)
        t4 = time.time_ns(); p4 = time.perf_counter_ns()
    if len(data) < 48:
        raise ValueError("short reply")
    f = struct.unpack('!BBBb11I', data[:48])
    li, mode, st = f[0] >> 6, f[0] & 7, f[1]
    if mode == 4 and st == 0:
        raise KoD(data[12:16].decode("ascii", "replace"))
    if mode != 4 or st > 15 or li == 3 or f[13] == 0:
        raise ValueError("unsynchronised reply")
    if (f[9], f[10]) != (sec, frac):
        raise ValueError("origin timestamp mismatch")
    t2, t3 = from_ntp(f[11], f[12]), from_ntp(f[13], f[14])
    off = ((t2 - t1) + (t3 - t4)) / 2 / 1e6
    delay = max(((p4 - p1) - (t3 - t2)) / 1e6, 0.0)
    return off, delay, st


def sample(spec, n=6, gap=0.8):
    host = spec.split(":")[0]
    if BACKOFF.get(host, 0) > time.time():
        return None
    for suf, (pn, pg) in LABS.items():
        if host.endswith(suf):
            n, gap = min(n, pn), max(gap, pg)
    try:
        addr = resolve(spec)  # resolve once so every sample hits the same pool member
    except Exception:
        return None
    best, ok, fails, offs = None, 0, 0, []
    for i in range(n):
        try:
            off, dl, st = query(addr)
            ok += 1; offs.append(off)
            if best is None or dl < best[1]:
                best = (off, dl, st)
        except KoD as e:
            BACKOFF[host] = time.time() + 900
            print(f"{host}: kiss-of-death '{e}', pausing 15 min", flush=True)
            break
        except Exception:
            fails += 1
            if ok == 0 and fails >= 2:
                break
        if i < n - 1:
            time.sleep(gap)
    if not best:
        return None
    return {"host": spec, "off": best[0], "delay": best[1], "stratum": best[2],
            "u": best[1] / 2 + 0.5, "ok": ok, "spread": max(offs) - min(offs), "rej": False}


def fuse(g):
    """Largest mutually consistent set (interval overlap, +/-5 ms slack). Offset = weighted mean
    clamped into the common interval. Uncertainty stays at the best single source, because path
    asymmetry on one phone connection is shared by all servers and does not average away."""
    g = [x for x in g if x and x["delay"] < 2500]
    if not g:
        return None
    n, best = len(g), None
    for c in g:
        x0 = c["off"] - c["u"] - 5
        act = [x for x in g if x["off"] - x["u"] - 5 <= x0 <= x["off"] + x["u"] + 5]
        key = (len(act), -min(x["u"] for x in act))
        if best is None or key > best[0]:
            best = (key, act)
    st = best[1]
    for x in g:
        x["rej"] = not any(x is y for y in st)
    conflict = n > 1 and len(st) == 1
    pick = st[0]["host"] if conflict else ""
    sw = sum(1 / (x["u"] ** 2) for x in st)
    off = sum(x["off"] / (x["u"] ** 2) for x in st) / sw
    lo, hi = max(x["off"] - x["u"] for x in st), min(x["off"] + x["u"] for x in st)
    if lo <= hi:
        off = min(max(off, lo), hi)
    return {"off": off, "u": min(x["u"] for x in st), "n": len(st), "tot": n,
            "conflict": conflict, "pick": pick, "src": g}


STATE = {"ready": False, "lock": threading.Lock()}


SAMPLES, GAP = 6, 0.8


def measure(servers):
    res, th = [None] * len(servers), []
    def job(i):
        res[i] = sample(servers[i], SAMPLES, GAP)
    for i in range(len(servers)):
        t = threading.Thread(target=job, args=(i,), daemon=True); t.start(); th.append(t)
    for t in th:
        t.join()
    return fuse(res), res


def short(spec):
    h, _, port = spec.partition(":")
    for suf in (".navy.mil", ".ntp.org", ".gov", ".jp", ".de", ".com"):
        if h.endswith(suf):
            h = h[:-len(suf)]
    h = h[5:] if h.startswith("time.") else h
    return h + (":" + port if port else "")


def describe(f, res, servers):
    ok = [(s, r) for s, r in zip(servers, res) if r]
    dead = [short(s) for s, r in zip(servers, res) if not r]
    head = f"NTP {f['n']}/{len(servers)} used" + (f"; CONFLICT, using {short(f['pick'])}" if f["conflict"] else "")
    body = " \u00b7 ".join(f"{short(s)} {r['off']:+.0f}\u00b1{r['u']:.0f}{' x' if r['rej'] else ''}" for s, r in ok)
    return head + " (ms, x = not used): " + body + (" | silent: " + ", ".join(dead) if dead else "")


def refresher(servers, interval):
    while True:
        f, res = measure(servers)
        if f:
            with STATE["lock"]:
                STATE.update(ready=True, off=f["off"], u=f["u"], t=time.perf_counter(),
                             note=describe(f, res, servers),
                             servers=[{"host": r["host"], "off": round(r["off"], 3), "u": round(r["u"], 3)}
                                      for r in res if r])
            print(time.strftime("%H:%M:%S"), STATE["note"], flush=True)
        else:
            print(time.strftime("%H:%M:%S"), "no NTP server reachable (UDP 123 blocked?)", flush=True)
        time.sleep(interval)


class H(BaseHTTPRequestHandler):
    def _h(self, code, body=b"", ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self._h(204)

    def do_GET(self):
        with STATE["lock"]:
            ok = STATE["ready"]; s = dict(STATE)
        if not ok:
            return self._h(503, b'{"error":"not ready"}')
        u = s["u"] + (time.perf_counter() - s["t"]) * 0.005  # 5 us/s growth
        ms = time.time_ns() / 1e6 + s["off"]               # stamped as late as possible
        body = json.dumps({"ms": round(ms, 3), "u": round(u, 3), "note": s["note"],
                           "servers": s["servers"]}).encode()
        self._h(200, body)

    def log_message(self, *a):
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--servers", default=",".join(DEFAULT))
    ap.add_argument("--port", type=int, default=8123)
    ap.add_argument("--interval", type=float, default=120)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--samples", type=int, default=6)
    ap.add_argument("--gap", type=float, default=0.8)
    a = ap.parse_args()
    global SAMPLES, GAP
    SAMPLES, GAP = a.samples, a.gap
    servers = [s.strip() for s in a.servers.split(",") if s.strip()]
    if a.once:
        f, res = measure(servers)
        if not f:
            print("no NTP server reachable"); sys.exit(1)
        print(describe(f, res, servers)); print(f"fused offset {f['off']:+.3f} ms  u \u00b1{f['u']:.3f} ms")
        return
    threading.Thread(target=refresher, args=(servers, a.interval), daemon=True).start()
    print(f"CHRONOS NTP companion on http://127.0.0.1:{a.port}/time  ({len(servers)} servers, {SAMPLES} samples each)", flush=True)
    ThreadingHTTPServer(("127.0.0.1", a.port), H).serve_forever()


if __name__ == "__main__":
    main()
