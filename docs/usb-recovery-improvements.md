# USB Recovery & Process Stability Improvements

This document summarises the changes made to improve resilience against USB hardware
failures observed running the container on a Synology NAS with an RTL-SDR dongle.

---

## Background

The application orchestrates two external binaries — `rtl_tcp` (USB SDR interface)
and `rtlamr` (meter decoder) — as async subprocesses. Several failure modes were
discovered through real-world log analysis that caused the container to shut down
unnecessarily. In all cases, simply restarting Docker restored normal operation,
indicating the failures were recoverable at the software level.

---

## Failure Modes Observed and Fixes Applied

### 1. rtl_tcp dies after the tickle, rtlamr retries never restart it

**Symptom:** After a sleep cycle wake-up, rtlamr reported
`connection refused` on every retry (all 5), then the container shut down.

**Root cause:** `rtl_tcp` is a single-client TCP server — it exits when its
connected client disconnects. The `tickle_rtl_tcp` call (which connects, sends
SDR commands, then closes) can kill rtl_tcp on some hardware. The original
wake-up code restarted rtl_tcp once before tickling, then ran all 5 rtlamr
retries without ever touching rtl_tcp again. Since rtl_tcp was dead for the
entire retry window, no amount of retrying rtlamr could succeed.

**Fix — `meter_reader.py` `_sleep_cycle`:** Replaced the single linear
start sequence with a `_MAX_WAKE_ATTEMPTS = 3` retry loop. Each attempt:
1. Starts rtl_tcp only if it is not already alive.
2. Tickles, then waits 1 second to let rtl_tcp settle (or die).
3. Checks `rtltcp.is_alive` — if it died from the tickle, loops back.
4. Starts rtlamr. On failure, stops rtl_tcp and retries the whole pair.

---

### 2. rtlamr dies mid-operation and rtl_tcp goes with it

**Symptom:** Shortly after a successful sleep-cycle wake-up, rtlamr exited
unexpectedly. All 5 restart retries got `connection refused`.

**Root cause:** When rtlamr exits, its TCP connection to rtl_tcp closes. rtl_tcp
then exits (client disconnect). The inline restart handler in `MeterReader.run()`
only called `rtlamr.start_with_retry()` — it never checked whether rtl_tcp was
also dead.

**Fix — `meter_reader.py` `run()` inline handler:** Before restarting rtlamr,
check `rtltcp.is_alive`. If rtl_tcp is also down, restart it first (with tickle
and 1-second settle), then restart rtlamr.

---

### 3. rtlamr silently hung — no output for hours

**Symptom:** After a successful wake-up, no readings were published and no errors
were logged for ~2 hours 43 minutes. The container eventually shut down (via an
external schedule restart). rtlamr was alive but producing no stdout output.
`MeterReader.run()` was blocked indefinitely on `await rtlamr.read_line()`.

**Root cause:** `asyncio.subprocess.Process.stdout.readline()` blocks forever
if the process is alive but has stopped writing. There was no timeout on reads.

**Fix — `meter_reader.py` `run()`:** Wrapped `rtlamr.read_line()` with
`asyncio.wait_for(..., timeout=_STUCK_TIMEOUT)` where `_STUCK_TIMEOUT = 600`
(10 minutes). On timeout: log a warning, stop both rtlamr and rtl_tcp, restart
the pair using the same tickle-and-check sequence as the sleep-cycle wake-up.

---

### 4. Startup timeout gave no diagnostic information

**Symptom:** Logs showed `rtlamr did not become ready within 30.0s` with no
indication of what rtlamr had actually printed before timing out (or whether it
had printed anything at all).

**Fix — `process_manager.py` `_wait_for_ready` / `start`:** Added a
`startup_lines` buffer that collects all stdout lines seen during startup.
On timeout, the error log now includes:
- The ready pattern that was being waited for.
- The last up to 20 lines rtlamr printed, or an explicit message that it produced
  no output at all (indicating a connection-level hang rather than a tuning issue).

---

### 5. Post-SIGKILL wait had no timeout — caused a 20-minute container hang

**Symptom:** After SIGKILL was sent to rtl_tcp, `rtl_tcp stopped` was never
logged and the container was unresponsive for ~20 minutes until manually restarted.

**Root cause:** On Synology NAS, when the RTL-SDR USB device enters a bad state,
the rtl_tcp process can enter kernel D-state (uninterruptible sleep on USB I/O).
A process in D-state cannot be killed even by SIGKILL. The code did
`await self._process.wait()` with no timeout after sending SIGKILL, so it blocked
forever.

**Fix — `process_manager.py` `stop()`:** Added `asyncio.wait_for(..., timeout=5.0)`
around the post-SIGKILL `process.wait()`. If the process is still alive after 5
seconds, log a clear `ERROR` explaining D-state and continue shutdown rather than
hanging indefinitely.

---

### 6. rtl_tcp required SIGKILL, leaving the USB device unreleased

User note: skeptical of the value of these changes

**Symptom:** After a sleep cycle, rtl_tcp consistently needed SIGKILL (did not
respond to SIGTERM in time). On wake-up, every rtl_tcp start attempt failed with
`usb_open error -4` and garbled device serial numbers (partial descriptor reads),
indicating the USB device was in a half-released state. A Docker restart (without
USB power cycling) immediately resolved the issue.

**Root cause:** `rtl_tcp` is blocked on a USB read when SIGTERM arrives. The
previous SIGTERM timeout was 2 seconds — too short if the event loop itself was
under load. After SIGKILL, the kernel does not always release the USB device
immediately within the same container lifetime, particularly on Synology NAS.

**Fixes:**

- **`process_manager.py` `stop()`:** Increased SIGTERM grace period from 2s to 5s,
  giving rtl_tcp more time to finish an in-flight USB read and exit cleanly.

---

## Files Changed

| File | Changes |
|---|---|
| `app/meter_reader.py` | Sleep cycle retry loop; inline rtl_tcp health check on rtlamr death; stuck-process watchdog (10 min); added debug logging throughout wake-up sequence |
| `app/process_manager.py` | Startup output buffer + dump on timeout; SIGTERM timeout 2s→5s; post-SIGKILL wait timeout (5s) to prevent D-state hang |
