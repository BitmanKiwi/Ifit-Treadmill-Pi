#!/usr/bin/env python3
"""
Treadmill Control Server
- BLE connect/disconnect on demand, survives page navigation
- Single asyncio.Lock guards the BLE radio
- Poll skips if a command holds the lock rather than queuing behind it
"""

import asyncio
import json
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from ifit.client import IFitBleClient

# ============================================================
# CONFIG
# ============================================================
MAC  = 'C3:E9:B7:6B:A0:21'
CODE = '0701e1d8ddd0d1d0d5e8e1f80d00112055486198bdd0f110354861b8cde0314098020000'

SETTLE_TIME        = 3.0
POLL_INTERVAL_FAST = 0.5
POLL_INTERVAL_IDLE = 3.0
WATCH_CHARS = ['CurrentKph', 'CurrentIncline', 'Mode', 'CurrentDistance', 'CurrentTime']

POST_STOP_HOLD_SECONDS = 15.0  # how long the register keeps its final value on screen
                                # after a stop before zeroing — cut short if a new
                                # workout's Mode goes RUNNING again first

WORKOUTS_FILE      = Path(__file__).parent / 'workouts.json'
REGISTER_LOG_FILE  = Path(__file__).parent / 'register_log.jsonl'
STATIC_DIR         = Path(__file__).parent / 'static'

# Mode values reported by the console over BLE.
MODE_IDLE    = 1
MODE_RUNNING = 2
MODE_PAUSED  = 3
MODE_SUMMARY = 4

# ============================================================
# BLE STATE
# ============================================================
class BleState:
    DISCONNECTED = 'disconnected'
    CONNECTING   = 'connecting'
    CONNECTED    = 'connected'


# ============================================================
# WORKOUT REGISTER
# ============================================================
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _append_register_log(entry: dict):
    try:
        with REGISTER_LOG_FILE.open('a') as f:
            f.write(json.dumps(entry) + '\n')
    except Exception as e:
        print(f'Register log write failed: {e}')


class WorkoutRegister:
    """
    Tracks distance/time as a ratcheting register, independent of any single
    BLE status read, so a skipped poll, a momentary zero/garbage read, a
    paused workout, or a dropped/reconnected BLE link never makes the
    displayed numbers sag or flicker.

    Register update rule, applied on every live status read:
      - a device reading greater than the register's current value moves
        the register up to match it.
      - a reading of zero, or one that isn't greater than the register, is
        ignored outright — this is what makes the register hold steady
        through a paused workout (which can report an empty/zero read), a
        BLE disconnect, or a poll skipped because a command holds the lock.
      - the register is zeroed once the treadmill's own Mode reports IDLE
        AND it has stayed there for POST_STOP_HOLD_SECONDS — never just
        because a read came back empty, and never while merely paused. This
        keeps the final distance/time on screen for a few seconds after a
        stop instead of blanking instantly, and a genuine new workout start
        resets the register immediately rather than waiting out the hold.

    Separately, this class turns raw `Mode` transitions into a clean
    started/paused/resumed/ended state machine (self.active / self.paused),
    distinguishing an explicit stop from a console auto-timeout out of a
    pause, and logs every transition — plus every BLE disconnect/reconnect
    that happens mid-workout — with a wall-clock timestamp to disk, so
    nothing has to be reconstructed after the fact from a watch file.
    """
    def __init__(self):
        self.active         = False
        self.paused         = False
        self.end_reason     = None   # 'stop' | 'auto_timeout' | None
        self.events         = []     # this workout's transitions, most recent last
        self._last_mode     = MODE_IDLE
        self.reg_distance   = 0.0    # ratcheting register — see class docstring
        self.reg_time       = 0
        self._stopped_at    = None   # monotonic time Mode first read IDLE — while set and
                                      # within POST_STOP_HOLD_SECONDS, the register holds
                                      # instead of zeroing
        self._explicit_stop = False  # set by Treadmill.stop() so its transient Mode 3→4→1
                                      # dance isn't misread as a paused-timeout end

    def to_dict(self) -> dict:
        return {
            'active'    : self.active,
            'paused'    : self.paused,
            'endReason' : self.end_reason,
            'eventCount': len(self.events),
        }

    def _log(self, event_type: str, distance, time_, **extra):
        entry = {
            'type': event_type, 'ts': _now_iso(),
            'deviceDistance': distance, 'deviceTime': time_,
            'regDistance': self.reg_distance, 'regTime': self.reg_time,
            **extra,
        }
        self.events.append(entry)
        _append_register_log(entry)
        print(f'Register: {event_type} @ dist={distance} time={time_} reg=({self.reg_distance},{self.reg_time}) {extra or ""}')
        return entry

    # ── Called on every successful live status read ─────────────
    # Returns (reg_distance, reg_time) — the ratcheting register, per the
    # update rule described in the class docstring.
    def on_mode(self, mode: int, distance, time_):
        prev = self._last_mode

        if mode == MODE_RUNNING and prev != MODE_RUNNING:
            if not self.active:
                self.active, self.paused, self.end_reason = True, False, None
                self.reg_distance, self.reg_time = 0.0, 0  # fresh workout — discard any
                self._stopped_at = None                     # post-stop hold left over from before
                self._log('start', distance, time_)
            elif self.paused:
                self.paused = False
                self._log('resume', distance, time_)

        elif mode == MODE_PAUSED and prev != MODE_PAUSED:
            if self.active and not self._explicit_stop:
                self.paused = True
                self._log('pause', distance, time_)

        elif mode in (MODE_IDLE, MODE_SUMMARY) and self.active and prev in (MODE_RUNNING, MODE_PAUSED):
            if self._explicit_stop:
                self.end_reason = 'stop'
            else:
                self.end_reason = 'auto_timeout' if prev == MODE_PAUSED else 'stop'
            self._explicit_stop = False
            self._log('end', distance, time_, reason=self.end_reason)
            self.active, self.paused = False, False

        # ── Register: ratchet up on a genuine forward reading, ignore
        # zero/non-increasing reads, zero out only after holding at IDLE
        # for POST_STOP_HOLD_SECONDS. ──
        if mode == MODE_IDLE:
            if self._stopped_at is None:
                self._stopped_at = time.monotonic()
            elif time.monotonic() - self._stopped_at >= POST_STOP_HOLD_SECONDS:
                self.reg_distance, self.reg_time = 0.0, 0
        else:
            self._stopped_at = None
            if distance is not None and distance != 0 and distance > self.reg_distance:
                self.reg_distance = distance
            if time_ is not None and time_ != 0 and time_ > self.reg_time:
                self.reg_time = time_

        self._last_mode = mode
        return self.reg_distance, self.reg_time

    # ── Called from Treadmill.connect()/disconnect() ────────────
    def on_ble_disconnect(self):
        if self.active:
            self._log('ble_disconnect', self.reg_distance, self.reg_time)

    def on_ble_reconnect(self, distance, time_):
        if self.active:
            self._log('ble_reconnect', distance, time_)

    # ── Called from Treadmill.stop() before it starts writing Mode ──
    def mark_explicit_stop(self):
        if self.active:
            self._explicit_stop = True


class Treadmill:
    def __init__(self):
        self.client         = None
        self.state          = BleState.DISCONNECTED
        self.lock           = asyncio.Lock()
        self.register       = WorkoutRegister()
        self._cached_status = self._make_status(False, 1, 0.0, 0.0, 0, 0)

    @property
    def is_connected(self):
        return self.state == BleState.CONNECTED

    # ── Connection ────────────────────────────────────────────
    async def connect(self):
        if self.state != BleState.DISCONNECTED:
            return
        self.state = BleState.CONNECTING
        print('BLE connecting...')
        await broadcast_ble_state()
        try:
            self.client = IFitBleClient(MAC, CODE)
            await self.client.connect()
            for attempt in range(30):
                await asyncio.sleep(1.0)
                try:
                    await self.client.read_characteristics(['Mode'])
                    print(f'BLE ready (attempt {attempt+1})')
                    break
                except Exception:
                    pass
            else:
                raise RuntimeError('Service discovery timeout')
            self.state = BleState.CONNECTED
            await broadcast_ble_state()
            ensure_poll()
            if self.register.active:
                try:
                    values = await self.client.read_characteristics(['CurrentDistance', 'CurrentTime'])
                    self.register.on_ble_reconnect(values.get('CurrentDistance'), values.get('CurrentTime'))
                except Exception:
                    self.register.on_ble_reconnect(None, None)
        except Exception as e:
            print(f'BLE connect failed: {e}')
            self.client = None
            self.state  = BleState.DISCONNECTED
            await broadcast_ble_state()
            raise

    async def disconnect(self):
        if self.state == BleState.DISCONNECTED:
            return
        self.state = BleState.DISCONNECTED
        self.register.on_ble_disconnect()
        try:
            if self.client:
                await self.client.disconnect()
        except Exception:
            pass
        self.client = None
        print('BLE disconnected')
        await broadcast_ble_state()

    # ── Commands ──────────────────────────────────────────────
    async def start(self, kph: float = 2.0):
        async with self.lock:
            await self.client.write_characteristics({'Mode': 2})
            if kph > 2.0:
                await asyncio.sleep(SETTLE_TIME)
                await self.client.set_speed(kph)

    async def pause(self):
        async with self.lock:
            await self.client.write_characteristics({'Mode': 3})

    async def resume(self, kph: float = 2.0):
        async with self.lock:
            await self.client.write_characteristics({'Mode': 2})
            if kph > 2.0:
                await asyncio.sleep(SETTLE_TIME)
                await self.client.set_speed(kph)

    async def stop(self):
        self.register.mark_explicit_stop()
        async with self.lock:
            await self.client.write_characteristics({'Mode': 3})
            await asyncio.sleep(1)
            await self.client.write_characteristics({'Mode': 4})
            await asyncio.sleep(1)
            await self.client.write_characteristics({'Mode': 1})

    async def set_speed(self, kph: float):
        kph = min(kph, 18.0)  # ProForm Carbon TL max speed
        print(f'set_speed: {kph}')
        async with self.lock:
            await self.client.set_speed(kph)

    async def set_incline(self, pct: float):
        pct = max(0.0, min(pct, 10.0))  # ProForm Carbon TL incline range
        async with self.lock:
            await self.client.set_incline(pct)

    # ── Status — skips poll if command holds the lock ─────────
    async def get_status(self) -> dict:
        if not self.is_connected or not self.client:
            return self._cached_status
        if self.lock.locked():
            return self._cached_status
        async with self.lock:
            try:
                values    = await self.client.read_characteristics(WATCH_CHARS)
                pulse_raw = values.get('Pulse', 0)
                pulse     = pulse_raw.get('pulse', 0) if isinstance(pulse_raw, dict) else pulse_raw
                mode      = values.get('Mode', 1)
                distance  = values.get('CurrentDistance', 0)
                time_     = values.get('CurrentTime', 0)
                distance, time_ = self.register.on_mode(mode, distance, time_)
                self._cached_status = self._make_status(
                    True,
                    mode,
                    values.get('CurrentKph', 0.0),
                    values.get('CurrentIncline', 0.0),
                    distance,
                    time_,
                    pulse,
                )
            except Exception as e:
                print(f'Status read error: {e}')
        return self._cached_status

    def _make_status(self, connected, mode, kph, incline, distance, time, pulse=0):
        return {
            'type'           : 'status',
            'bleState'       : self.state,
            'connected'      : connected,
            'mode'           : mode,
            'currentKph'     : kph,
            'currentIncline' : incline,
            'currentDistance': distance,
            'currentTime'    : time,
            'pulse'          : pulse,
            'register'       : self.register.to_dict(),
        }


# ============================================================
# WORKOUT STORAGE
# ============================================================
def load_workouts() -> list:
    if WORKOUTS_FILE.exists():
        try:
            return json.loads(WORKOUTS_FILE.read_text())
        except Exception:
            return []
    return []

def save_workouts(workouts: list):
    WORKOUTS_FILE.write_text(json.dumps(workouts, indent=2))


# ============================================================
# APP STATE
# ============================================================
treadmill = Treadmill()
clients: set[WebSocket] = set()
poll_task = None


# ============================================================
# BROADCAST
# ============================================================
async def broadcast(data: dict):
    if not clients:
        return
    msg  = json.dumps(data)
    dead = set()
    for ws in clients:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    clients.difference_update(dead)

async def broadcast_ble_state():
    await broadcast({'type': 'ble_state', 'bleState': treadmill.state})


# ============================================================
# POLL LOOP
# ============================================================
async def poll_loop():
    # Runs off BLE connection state, not client presence — this is what keeps
    # status polling going even when no browser is currently connected,
    # e.g. a phone/PC session that went to sleep.
    global poll_task
    while treadmill.is_connected:
        status   = await treadmill.get_status()
        await broadcast(status)  # no-ops harmlessly if clients is empty
        interval = POLL_INTERVAL_FAST if status.get('mode', 1) in (2, 3) else POLL_INTERVAL_IDLE
        await asyncio.sleep(interval)
    poll_task = None

def ensure_poll():
    global poll_task
    if poll_task is None or poll_task.done():
        poll_task = asyncio.create_task(poll_loop())


# ============================================================
# LIFESPAN
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await treadmill.disconnect()


# ============================================================
# APP
# ============================================================
app = FastAPI(lifespan=lifespan)
STATIC_DIR.mkdir(exist_ok=True)
app.mount('/static', StaticFiles(directory=STATIC_DIR), name='static')


# ============================================================
# ROUTES
# ============================================================
@app.get('/')
async def manual_page():
    return FileResponse(STATIC_DIR / 'treadmill_manual.html')

@app.get('/workout')
async def workout_page():
    return FileResponse(STATIC_DIR / 'treadmill_workout.html')

@app.get('/build')
async def build_page():
    return FileResponse(STATIC_DIR / 'treadmill_build.html')

@app.get('/monitor')
async def monitor_page():
    return FileResponse(STATIC_DIR / 'treadmill_monitor.html')

@app.get('/api/workouts')
async def get_workouts():
    return JSONResponse(load_workouts())

@app.post('/api/workouts')
async def save_workout(workout: dict):
    workouts = load_workouts()
    existing = next((i for i, w in enumerate(workouts) if w.get('id') == workout.get('id')), None)
    if existing is not None:
        workouts[existing] = workout
    else:
        workouts.append(workout)
    save_workouts(workouts)
    return JSONResponse({'ok': True})

@app.delete('/api/workouts/{workout_id}')
async def delete_workout(workout_id: str):
    workouts = [w for w in load_workouts() if w.get('id') != workout_id]
    save_workouts(workouts)
    return JSONResponse({'ok': True})


# ============================================================
# WEBSOCKET
# ============================================================
@app.websocket('/ws')
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    print(f'WS client connected ({len(clients)} total)')

    await ws.send_text(json.dumps({'type': 'ble_state', 'bleState': treadmill.state}))
    if treadmill.is_connected:
        await ws.send_text(json.dumps(await treadmill.get_status()))

    ensure_poll()

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            cmd = msg.get('cmd')
            print(f'WS cmd: {cmd}')

            if cmd == 'connect':
                asyncio.create_task(_do_connect())
                continue

            if cmd == 'disconnect':
                await treadmill.disconnect()
                continue

            if not treadmill.is_connected:
                await ws.send_text(json.dumps({'type': 'error', 'message': 'Not connected'}))
                continue

            try:
                if cmd == 'start':
                    await treadmill.start(float(msg.get('value', 2.0)))
                elif cmd == 'pause':
                    await treadmill.pause()
                elif cmd == 'resume':
                    await treadmill.resume(float(msg.get('value', 2.0)))
                elif cmd == 'stop':
                    await treadmill.stop()
                elif cmd == 'speed':
                    await treadmill.set_speed(float(msg.get('value', 2.0)))
                elif cmd == 'incline':
                    await treadmill.set_incline(float(msg.get('value', 0.0)))
            except Exception as e:
                print(f'Command error: {e}')
                await ws.send_text(json.dumps({'type': 'error', 'message': str(e)}))

    except WebSocketDisconnect:
        clients.discard(ws)
        print(f'WS client disconnected ({len(clients)} remaining)')


async def _do_connect():
    try:
        await treadmill.connect()
        if treadmill.is_connected:
            await broadcast(await treadmill.get_status())
    except Exception as e:
        await broadcast({'type': 'error', 'message': f'Connect failed: {e}'})


# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    uvicorn.run(
        'server:app',
        host='0.0.0.0',
        port=80,
        log_level='info',
    )