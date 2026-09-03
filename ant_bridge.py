"""
ant_bridge.py — ANT+ SDM footpod broadcast for the treadmill Pi.

Runs two ANT+ channels on a single USB stick:
  - Channel 0 (slave/RX): passthrough of your HRM-Pro heart rate strap
    (same pattern as your working ant_hr.py)
  - Channel 1 (master/TX): broadcasts speed/distance as an ANT+
    Stride-Based Speed and Distance Monitor (SDM) "footpod", so a
    Garmin Forerunner can pair to the Pi and display it.

TWO WAYS TO RUN THIS:
  1. Standalone (blocking, simulated data) — for isolated testing without
     touching server.py at all:
         python3 ant_bridge.py
     Broadcasts a fixed, known STABLE_KPH so pace/distance on the watch
     can be checked against simple arithmetic.
  2. Embedded (non-blocking, real treadmill data) — imported and started
     by server.py via start_ant_bridge(treadmill):
         from ant_bridge import start_ant_bridge
         start_ant_bridge(treadmill)
     Reads treadmill._cached_status directly on every SDM broadcast
     (~4Hz) — no WebSocket, no polling-interval staleness. See
     start_ant_bridge()'s docstring for why this is coupled directly to
     the Treadmill object rather than going through the WebSocket that
     server.py already broadcasts on: poll_loop() only updates every
     0.5-3s, far slower than the ANT+ channel's own transmit rate, which
     would make the footpod visibly step rather than update smoothly.
  Both modes share the same node/channel setup and SDM encoding via
  _run_node() — only the data source differs.

====================================================================
STATUS: fully verified against your installed openant source
====================================================================

CONFIRMED against real sources on the Pi:
  - Node(), node.set_network_key()      — from your working ant_hr.py
  - node.new_channel(ctype, network_number=0x00, ext_assign=None) —
    confirmed exact signature; our call (network_number defaults to
    0x00, matching the network key we set) is correct as written
  - channel.set_period/set_rf_freq/set_id/open() — confirmed in the
    real Channel class source
  - channel.on_broadcast_tx_data       — confirmed real, AND confirmed
    it fires correctly: Node._worker_event() maps the hardware's
    EVENT_TX straight through to this callback via the main loop.
    (An older 2021 GitHub issue reported this never firing on some
    setup/version — the source here shows it works, so that's resolved
    on this openant version.)
  - Channel.Type.BIDIRECTIONAL_TRANSMIT (0x10) — confirmed correct
    enum for a master/broadcast channel ("AKA master" in source)
  - send_broadcast_data(data: List[int]) — confirmed real method +
    signature; payload converted to a plain list to match exactly
  - device_type 124 for StrideSpeed — independently confirmed by
    `openant scan` correctly identifying your own HR strap's built-in
    stride/speed broadcast as DeviceType.StrideSpeed

FIXED: on_broadcast_tx_data's real signature takes a `data` argument
  (Node._main() calls it as channel.on_broadcast_tx_data(data)) — an
  earlier draft defined the callback with zero parameters, which would
  have thrown a TypeError on the very first transmit. Fixed below.

FIRST LIVE TEST RESULT: pairing succeeded (device 12345 found and
  paired as a footpod on the Forerunner) but no speed data displayed.
  Added common pages 80/81 + double-page cycling per Nordic's docs.

CALIBRATION TESTS (3x, varying only a speed "resolution" constant):
  10.0 kph fixed showed 2:25/km, then 3:20/km, then 1:55/km. These
  don't fit ANY single linear scaling factor (implied resolution was
  different each time: 0.0097, 0.0181, 0.0568 m/s/unit) — ruled out
  a pure scaling error. Confirmed deterministic (re-ran test 1's exact
  config twice, got 2:25/km both times) — not watch-side flakiness.

ROOT CAUSE FOUND — byte layout was structurally wrong, not just
  mis-scaled. Verified against a real, working, actively-maintained
  implementation (not a prose description):
  https://github.com/Loghorn/ant-plus/blob/master/src/stride-speed-distance-sensors.ts
  (source comments cite the official ANT+ profile page + spec sheet
  at thisisant.com). Three structural errors found and fixed:
    1. Speed was a 16-bit LE value at bytes 4-5. Real layout: NIBBLE-
       packed — lower 4 bits of byte 4 = whole m/s (0-15), byte 5 =
       fractional m/s (1/256 resolution).
    2. Distance was one rolling byte at 0.1m resolution. Real layout:
       byte 3 = whole metres (rolls at 256m), upper nibble of byte 4
       = 1/16m fractional precision.
    3. Time was a single rolling byte. Real layout: byte 1 = 1/200s
       fractional (had this right) PLUS byte 2 = a separate whole-
       seconds counter (rolls at 256s) — we never populated this;
       our old byte 2 held distance data instead. This field
       collision (receiver reading distance as "seconds elapsed") is
       the most likely explanation for the non-linear, inconsistent
       results across all three calibration tests. Rewritten below.
  Also independently confirms SDM_CHANNEL_PERIOD=8134 is correct —
  the real source uses the exact same constant.

NOT YET RE-TESTED against the watch with this corrected layout.
====================================================================
"""

import threading
import time

from openant.easy.node import Node
from openant.easy.channel import Channel
from openant.devices import ANTPLUS_NETWORK_KEY
from openant.devices.heart_rate import HeartRate, HeartRateData

# ── Config ───────────────────────────────────────────────────────
HR_DEVICE_ID = 8925          # your HRM-Pro's ANT+ device number (from ant_hr.py)
SDM_DEVICE_ID = 12345        # arbitrary ID for this virtual footpod — any 1-65535,
                              # just avoid clashing with a real device you own
SDM_DEVICE_TYPE = 124        # ANT+ "Stride-Based Speed and Distance Sensors"
SDM_TRANS_TYPE = 5           # commonly used independent-channel transmission type
SDM_CHANNEL_PERIOD = 8134    # ANT+ SDM standard period, ~4.03 Hz message rate
SDM_RF_FREQ = 57             # 2457 MHz — standard ANT+ frequency (all ANT+ profiles)
                              # NOTE: SDM_CHANNEL_PERIOD (8134) is independently
                              # confirmed correct — it matches the exact same
                              # constant in the verified real-world source below.

# DIAG — set False to run RX-only (no SDM channel opened at all), to isolate
# whether the SDM TX channel is the cause of HR RX staleness. Strip this
# whole DIAG block (search "DIAG") once the timing question is answered.
ENABLE_SDM_TX = True

# ── Live HR state, updated by the HR strap's RX callback ────────────
class _HRState:
    def __init__(self):
        self.bpm = 0
        self.found = False        # True once the ANT+ channel has ever
                                    # detected the strap (on_found fired)
        self.last_update = None   # time.time() of the last bpm reading,
                                    # or None if never received one
        self.lock = threading.Lock()

    def mark_found(self):
        with self.lock:
            self.found = True

    def update(self, bpm):
        with self.lock:
            self.bpm = bpm
            self.last_update = time.time()

    def get(self):
        with self.lock:
            return self.bpm

    def debug_snapshot(self):
        with self.lock:
            age = (time.time() - self.last_update) if self.last_update else None
            return {"bpm": self.bpm, "found": self.found, "ageSeconds": age}


_hr_state = _HRState()


# DIAG — tracks when the SDM TX callback last returned, and how long its
# send_broadcast_data() call took, plus a running record of the gap between
# TX returns and the next HR reading (last + worst-case since bridge start),
# so both the console log and get_hr_debug() can report it. Strip this whole
# DIAG block once the timing question is answered.
class _TxTiming:
    def __init__(self):
        self.last_tx_return = None
        self.last_tx_duration = None
        self.last_hr_gap = None       # seconds — most recent HR reading's gap
        self.max_hr_gap = None        # seconds — worst gap seen since start
        self.max_hr_gap_at = None     # time.time() when that max occurred
        self.lock = threading.Lock()

    def record_tx(self, duration):
        with self.lock:
            self.last_tx_return = time.time()
            self.last_tx_duration = duration

    def gap_since_last_tx(self):
        with self.lock:
            if self.last_tx_return is None:
                return None, None
            return time.time() - self.last_tx_return, self.last_tx_duration

    def record_hr_gap(self, gap):
        with self.lock:
            self.last_hr_gap = gap
            if self.max_hr_gap is None or gap > self.max_hr_gap:
                self.max_hr_gap = gap
                self.max_hr_gap_at = time.time()

    def debug_snapshot(self):
        with self.lock:
            return {
                "lastTxDurationMs": round(self.last_tx_duration * 1000, 1)
                    if self.last_tx_duration is not None else None,
                "lastHrGapMs": round(self.last_hr_gap * 1000, 1)
                    if self.last_hr_gap is not None else None,
                "maxHrGapMs": round(self.max_hr_gap * 1000, 1)
                    if self.max_hr_gap is not None else None,
                "maxHrGapAgeSeconds": (time.time() - self.max_hr_gap_at)
                    if self.max_hr_gap_at is not None else None,
            }


_tx_timing = _TxTiming()


def get_latest_hr():
    """Returns the most recent heart rate reading (bpm) from the ANT+ HR
    strap, or 0 if none received yet (strap not worn/out of range/ant_bridge
    not running). Call this from server.py to include real HR data in the
    broadcast status — thread-safe, no import-time dependency on the ANT+
    node actually having started successfully."""
    return _hr_state.get()


def get_hr_debug():
    """Returns {bpm, found, ageSeconds} for on-screen diagnostics — 'found'
    is True once the channel has ever detected the strap at all (even if
    readings have since gone stale), 'ageSeconds' is how long ago the last
    reading arrived (None if never). Lets you tell 'strap never paired'
    apart from 'was working, now stale' without SSHing in to check logs.

    DIAG — also includes lastTxDurationMs, lastHrGapMs, maxHrGapMs,
    maxHrGapAgeSeconds (see _TxTiming above) and sdmTxEnabled (mirrors
    ENABLE_SDM_TX), so the TX/RX timing test is visible anywhere
    get_hr_debug() is already surfaced, not just the console log. Strip
    once the timing question is answered."""
    snapshot = _hr_state.debug_snapshot()
    snapshot.update(_tx_timing.debug_snapshot())   # DIAG
    snapshot["sdmTxEnabled"] = ENABLE_SDM_TX        # DIAG
    return snapshot


# ── Shared live state, updated by the simulation thread ─────────────
class LiveState:
    def __init__(self):
        self.kph = 0.0
        self.distance_m = 0.0
        self.lock = threading.Lock()

    def update(self, kph, distance_m):
        with self.lock:
            self.kph = kph
            self.distance_m = distance_m

    def snapshot(self):
        with self.lock:
            return self.kph, self.distance_m


live = LiveState()


STABLE_KPH = 30.0             # fixed test speed — 2:00/km pace, 3x faster than the
                              # original 10 kph test to reveal faster whether the small
                              # ~1.4% discrepancy (6:05 vs 6:00 at 10kph) scales with
                              # speed or stays a fixed error. NOTE: can't test a literal
                              # 1:00/km (60kph/16.67 m/s) — the SDM speed field's integer
                              # part is a 4-bit nibble, hard-capped at 15 m/s (~57.6kph,
                              # ~1:02.5/km) by the real protocol, not by us.


def start_simulation_thread():
    """Broadcasts a FIXED, known speed with real-time distance accumulation —
    deliberately not varying, so any drift/wrongness on the watch is purely a
    scaling/encoding issue, not you eyeballing a moving target."""

    def runner():
        last_tick = time.time()
        distance_m = 0.0
        while True:
            time.sleep(0.25)
            now = time.time()
            dt = now - last_tick
            last_tick = now
            distance_m += (STABLE_KPH / 3.6) * dt
            live.update(STABLE_KPH, distance_m)

    threading.Thread(target=runner, daemon=True).start()
    print(f"[ant_bridge] CALIBRATION MODE — fixed {STABLE_KPH} kph, distance accumulating in real time")


# ── SDM page encoding ────────────────────────────────────────────
# VERIFIED against a real, working, actively-maintained implementation:
# https://github.com/Loghorn/ant-plus/blob/master/src/stride-speed-distance-sensors.ts
# (its own source comments cite the official ANT+ profile page and spec
# sheet at thisisant.com). This replaces our earlier best-effort guess,
# which had THREE structural errors, not just a wrong scaling constant:
#   1. Speed was a 16-bit LE value at bytes 4-5. Real layout: speed is a
#      NIBBLE-packed value — lower 4 bits of byte 4 = whole m/s (0-15
#      only), byte 5 = fractional m/s (1/256 resolution).
#   2. Distance was one rolling byte at 0.1m resolution. Real layout:
#      byte 3 = whole metres (rolls at 256m), upper nibble of byte 4 =
#      1/16m fractional precision.
#   3. Time was a single rolling byte. Real layout: byte 1 = fractional
#      (1/200s, which we had right) AND byte 2 = a SEPARATE whole-
#      seconds counter (rolls at 256s), which we never populated —
#      our old byte 2 held distance data instead. This field collision
#      (receiver reading our distance byte as "seconds elapsed") is the
#      most likely explanation for the wildly inconsistent, non-linear
#      pace results across every prior test.

_sdm_start_time = time.time()
MAX_NIBBLE_SPEED_MPS = 15    # speed integer part is 4 bits — cap here to avoid
                              # wrapping garbage at unrealistic treadmill speeds


def _speed_nibble_and_frac(kph):
    """Returns (speed_int_0_15, speed_fractional_byte) per the verified layout."""
    speed_mps = min(kph / 3.6, MAX_NIBBLE_SPEED_MPS + 0.996)
    speed_int = int(speed_mps)
    speed_frac = round((speed_mps - speed_int) * 256)
    if speed_frac >= 256:            # rare rounding overflow (frac ~0.998+) — carry
        speed_frac = 0
        speed_int += 1
    return speed_int & 0x0F, speed_frac & 0xFF


def _encode_page1(kph, distance_m):
    """Page 1 — main data: two-part time, two-part distance, nibble-packed
    speed, stride count, update latency. Byte-exact per verified source."""
    elapsed = time.time() - _sdm_start_time
    time_fractional = int((elapsed * 200) % 256)         # 1/200s, rolls over —
                                                            # kept as floor: this is
                                                            # a rolling accumulated
                                                            # counter (Eq 7-1 style),
                                                            # not a precision refinement
    time_integer = int(elapsed) % 256                     # whole seconds, rolls at 256s

    distance_integer = int(distance_m) % 256               # whole metres, rolls at 256m —
                                                              # also kept as floor, same
                                                              # accumulated-counter reasoning
    dist_frac_raw = round((distance_m % 1) * 16)
    if dist_frac_raw >= 16:           # rare rounding overflow — carry into whole metres
        dist_frac_raw = 0
        distance_integer = (distance_integer + 1) % 256
    distance_fractional = dist_frac_raw & 0x0F              # 1/16m precision — this IS
                                                                # a pure precision refinement
                                                                # of the current value, so
                                                                # rounding (not flooring) is
                                                                # correct here. CHANGED from
                                                                # int()/floor after a 6:05/km
                                                                # result (vs target 6:00) —
                                                                # floor's one-directional bias
                                                                # on this + speed_frac was the
                                                                # likely ~1.4% culprit.

    speed_int, speed_frac = _speed_nibble_and_frac(kph)
    byte4 = (distance_fractional << 4) | speed_int          # packed nibble

    stride_count = 0     # not available from the treadmill
    update_latency = 0   # no meaningful latency tracking on our end — 0 is the
                          # conventional "not applicable" value for this field

    return bytes([
        0x01,
        time_fractional,
        time_integer,
        distance_integer,
        byte4,
        speed_frac,
        stride_count,
        update_latency,
    ])



def _encode_page2(kph):
    """Page 2 — status/cadence page: cadence (n/a, sent as 0), nibble-packed
    speed (same fields as page 1 so partial-page receivers stay current),
    status byte. Byte-exact per verified source (parser's page 2-15 path)."""
    cadence_integer = 0        # not available from the treadmill
    cadence_fractional = 0
    speed_int, speed_frac = _speed_nibble_and_frac(kph)
    byte4 = (cadence_fractional << 4) | speed_int

    status_byte = 0x01   # best-effort placeholder — real bit meanings not
                          # confirmed by this source, parser stores it raw

    return bytes([
        0x02,
        0x00,   # reserved (parser doesn't read byte 1 for this page range)
        0x00,   # reserved (parser doesn't read byte 2 for this page range)
        cadence_integer,
        byte4,
        speed_frac,
        0x00,   # reserved for page 2 specifically (only page 3 uses byte 6, for Calories)
        status_byte,
    ])


# Common pages 80/81 — NOT SDM-specific, these are standard ANT+ background
# pages required across virtually all ANT+ sensor profiles for a receiver to
# fully validate/commission the sensor. Nordic's SDM TX example explicitly
# lists "Transmit common page 80 and 81" as a compliance requirement, sent
# every 65th message. Much better-documented/standardized than the SDM-
# specific pages, so higher confidence these are byte-exact.

def _encode_page80():
    """Page 80 — Manufacturer Identification (standard ANT+ common page)."""
    return bytes([
        0x50,   # page number 80
        0xFF, 0xFF,             # reserved
        0x01,                   # HW revision (arbitrary — 1)
        0xFF, 0xFF,              # Manufacturer ID (0xFFFF = "unset/dev")
        0xFF, 0xFF,              # Model number (0xFFFF = "unset/dev")
    ])


def _encode_page81():
    """Page 81 — Product Information (standard ANT+ common page)."""
    return bytes([
        0x51,   # page number 81
        0xFF,                    # reserved
        0xFF,                    # supplemental SW revision (none)
        0x01,                    # SW revision (arbitrary — 1)
        0x01, 0x00, 0x00, 0x00,  # serial number (arbitrary — 1), LE uint32
    ])


# ── ANT+ node setup ──────────────────────────────────────────────
def on_hr_found():
    _hr_state.mark_found()
    print(f"[ant_bridge] HR strap found: device_id={hr_device.device_id}")


def on_hr_data(data: HeartRateData):
    gap, tx_dur = _tx_timing.gap_since_last_tx()   # DIAG
    if gap is not None:                            # DIAG
        _tx_timing.record_hr_gap(gap)               # DIAG
        print(f"[ant_bridge][diag] HR callback fired {gap*1000:.1f}ms after last TX "
              f"return (that TX call took {tx_dur*1000:.1f}ms)")
    _hr_state.update(data.heart_rate)
    print(f"[ant_bridge] HR: {data.heart_rate} bpm")


def _get_live_from_treadmill(treadmill):
    """Reads live kph/distance directly off the Treadmill object's cached
    status — always the freshest value available (updated by poll_loop()
    on every successful BLE read), no WebSocket/JSON round-trip and no
    polling-interval staleness between this and the SDM channel's own
    ~4Hz transmit rate. Thread-safe without a lock: _cached_status is
    reassigned wholesale (never mutated in place) from server.py's
    asyncio event loop thread, and CPython's GIL makes a single attribute
    read/write atomic — this will only ever see a fully-old or fully-new
    dict, never a torn one."""
    status = treadmill._cached_status
    return status.get('currentKph', 0.0), status.get('currentDistance', 0.0)


def _run_node(get_live_data, blocking):
    """Builds the ANT+ node (HR passthrough channel + SDM TX channel) and
    starts it. get_live_data() is called on every SDM broadcast (~4Hz) to
    fetch (kph, distance_m) — the only thing that differs between
    standalone/simulated and embedded/real-treadmill modes.

    blocking=True: calls node.start() directly on the current thread/
      process (blocks forever, only returns on Ctrl+C/KeyboardInterrupt).
      For standalone use (`python3 ant_bridge.py`).
    blocking=False: runs node.start() in a background daemon thread and
      returns immediately. For embedding in server.py — node/channel
      setup happens synchronously first, so a real setup failure (e.g.
      missing USB dongle) still raises out of this call for the caller's
      try/except to catch; only a failure inside the already-running
      node.start() loop itself won't propagate (logged instead).
    """
    global hr_device

    node = Node()
    node.set_network_key(0x00, ANTPLUS_NETWORK_KEY)

    # ── Channel 0 — HR strap (slave/RX) — same as your working ant_hr.py ──
    hr_device = HeartRate(node, device_id=HR_DEVICE_ID)
    hr_device.on_found = on_hr_found
    hr_device.on_device_data = lambda page, page_name, data: on_hr_data(data)

    # ══════════════════════════════════════════════════════════════
    # ── Channel 1 — SDM footpod broadcast (master/TX) ──────────────
    # CONFIRMED against the real installed openant Channel AND Node source:
    #   - BIDIRECTIONAL_TRANSMIT (0x10) is explicitly commented "AKA
    #     master" in the source — correct enum for a broadcast/TX channel
    #   - send_broadcast_data(data: List[int]) is the real method name
    #     and signature — wants a list of ints, so we convert explicitly
    #   - node.new_channel(ctype, network_number=0x00, ext_assign=None) —
    #     confirmed exact signature; network_number default (0x00) matches
    #     the network key we set above
    # ══════════════════════════════════════════════════════════════
    # DIAG — sdm_channel stays None in RX-only mode (ENABLE_SDM_TX=False)
    sdm_channel = None
    if ENABLE_SDM_TX:
        sdm_channel = node.new_channel(Channel.Type.BIDIRECTIONAL_TRANSMIT)
        sdm_channel.set_id(SDM_DEVICE_ID, SDM_DEVICE_TYPE, SDM_TRANS_TYPE)
        sdm_channel.set_period(SDM_CHANNEL_PERIOD)
        sdm_channel.set_rf_freq(SDM_RF_FREQ)

    # Cycling pattern per Nordic's documented SDM TX behaviour: each data
    # page sent TWICE in a row before switching (page1,page1,page2,page2,...)
    # for compatibility with lower-rate receivers, with common pages 80/81
    # injected every 65th message.
    _tx_state = {"msg_count": 0, "page_pair": 0, "page_rep": 0}

    def on_sdm_broadcast_tx(data=None):
        # openant calls this with a `data` arg (from Node._main()'s
        # EVENT_TX handling) every time the channel needs its next
        # payload. We don't use the incoming `data` (not meaningful for
        # a TX-only channel).
        st = _tx_state
        st["msg_count"] += 1

        if st["msg_count"] % 65 == 0:
            # inject a common page — alternate 80/81 each time this fires
            payload = _encode_page80() if (st["msg_count"] // 65) % 2 == 0 else _encode_page81()
        else:
            kph, distance_m = get_live_data()
            payload = _encode_page1(kph, distance_m) if st["page_pair"] == 0 else _encode_page2(kph)
            st["page_rep"] += 1
            if st["page_rep"] >= 2:      # sent this page twice — switch to the other
                st["page_rep"] = 0
                st["page_pair"] ^= 1

        _t0 = time.time()                                          # DIAG
        sdm_channel.send_broadcast_data(list(payload))   # confirmed: wants List[int]
        _tx_timing.record_tx(time.time() - _t0)                    # DIAG

    if ENABLE_SDM_TX:                             # DIAG
        sdm_channel.on_broadcast_tx_data = on_sdm_broadcast_tx
        sdm_channel.open()
    else:                                          # DIAG
        print("[ant_bridge] DIAG: ENABLE_SDM_TX=False — RX-only run, no SDM channel opened")
    # ══════════════════════════════════════════════════════════════

    def _start_and_cleanup():
        try:
            node.start()
        except KeyboardInterrupt:
            print("\n[ant_bridge] stopping...")
        finally:
            hr_device.close_channel()
            if sdm_channel is not None:   # DIAG
                sdm_channel.close()
            node.stop()

    if blocking:
        print("[ant_bridge] starting ANT+ node (HR RX on ch0, SDM TX on ch1)...")
        _start_and_cleanup()
    else:
        print("[ant_bridge] starting ANT+ node in background thread (HR RX on ch0, SDM TX on ch1)...")
        threading.Thread(target=_start_and_cleanup, daemon=True, name="ant-bridge").start()

    return node, hr_device, sdm_channel


def start_ant_bridge(treadmill):
    """Entry point for server.py. Starts the ANT+ bridge — HR strap
    passthrough plus an SDM footpod broadcast fed from LIVE treadmill
    state — in a background thread, and returns immediately without
    blocking server startup.

    Reads treadmill._cached_status directly on every ~0.25s SDM transmit
    (see _get_live_from_treadmill) rather than going through the
    WebSocket: poll_loop() only broadcasts every 0.5-3s, which would make
    the footpod visibly step between stale values instead of updating
    smoothly at the ANT+ channel's own ~4Hz rate.

    Call this wrapped in try/except from server.py's lifespan — a setup
    failure here (missing USB dongle, permissions, etc.) must never
    prevent the treadmill control server itself from starting.
    """
    print("[ant_bridge] starting in embedded mode — reading live treadmill state directly")
    return _run_node(lambda: _get_live_from_treadmill(treadmill), blocking=False)


def main():
    """Standalone entry point — simulated data, blocks this process.
    Run directly: python3 ant_bridge.py"""
    start_simulation_thread()
    _run_node(live.snapshot, blocking=True)


if __name__ == "__main__":
    main()