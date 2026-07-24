#!/usr/bin/env python3
"""
Treadmill Control Server
- BLE connect/disconnect on demand, survives page navigation
- Single asyncio.Lock guards the BLE radio
- Poll skips if a command holds the lock rather than queuing behind it
"""

import asyncio
import json
from contextlib import asynccontextmanager
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

# Resend the current speed as a BLE write every this many metres while running,
# to stop the console/BLE link from timing out during long steady-state efforts
# where no user commands are being sent.
KEEPALIVE_DISTANCE_M = 1000.0

WORKOUTS_FILE = Path(__file__).parent / 'workouts.json'
STATIC_DIR    = Path(__file__).parent / 'static'

# ============================================================
# BLE STATE
# ============================================================
class BleState:
    DISCONNECTED = 'disconnected'
    CONNECTING   = 'connecting'
    CONNECTED    = 'connected'


class Treadmill:
    def __init__(self):
        self.client         = None
        self.state          = BleState.DISCONNECTED
        self.lock           = asyncio.Lock()
        self._cached_status = self._make_status(False, 1, 0.0, 0.0, 0, 0)
        self._last_cmd_kph     = 2.0   # most recently commanded speed, resent for keepalive
        self._keepalive_dist_m = 0.0   # distance (m) at which the last keepalive write was sent

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
            self._last_cmd_kph     = kph
            self._keepalive_dist_m = 0.0

    async def pause(self):
        async with self.lock:
            await self.client.write_characteristics({'Mode': 3})

    async def resume(self, kph: float = 2.0):
        async with self.lock:
            await self.client.write_characteristics({'Mode': 2})
            if kph > 2.0:
                await asyncio.sleep(SETTLE_TIME)
                await self.client.set_speed(kph)
            self._last_cmd_kph     = kph
            self._keepalive_dist_m = self._cached_status.get('currentDistance', 0.0)

    async def stop(self):
        async with self.lock:
            await self.client.write_characteristics({'Mode': 3})
            await asyncio.sleep(1)
            await self.client.write_characteristics({'Mode': 4})
            await asyncio.sleep(1)
            await self.client.write_characteristics({'Mode': 1})
            self._keepalive_dist_m = 0.0

    async def set_speed(self, kph: float):
        kph = min(kph, 18.0)  # ProForm Carbon TL max speed
        print(f'set_speed: {kph}')
        async with self.lock:
            await self.client.set_speed(kph)
            self._last_cmd_kph = kph

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
                self._cached_status = self._make_status(
                    True,
                    mode,
                    values.get('CurrentKph', 0.0),
                    values.get('CurrentIncline', 0.0),
                    distance,
                    values.get('CurrentTime', 0),
                    pulse,
                )

                # BLE keepalive — resend current speed as a write (not just a read)
                # every KEEPALIVE_DISTANCE_M while running, so the console/BLE link
                # doesn't idle-timeout during long steady-state efforts.
                if mode == 2 and (distance - self._keepalive_dist_m) >= KEEPALIVE_DISTANCE_M:
                    try:
                        await self.client.set_speed(self._last_cmd_kph)
                        self._keepalive_dist_m = distance
                        print(f'BLE keepalive: resent {self._last_cmd_kph} kph at {distance}m')
                    except Exception as e:
                        print(f'BLE keepalive write failed: {e}')
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
    global poll_task
    while clients:
        if treadmill.is_connected:
            status   = await treadmill.get_status()
            await broadcast(status)
            interval = POLL_INTERVAL_FAST if status.get('mode', 1) in (2, 3) else POLL_INTERVAL_IDLE
        else:
            interval = POLL_INTERVAL_IDLE
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
