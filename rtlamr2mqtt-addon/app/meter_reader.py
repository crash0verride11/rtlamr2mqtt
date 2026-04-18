"""
Async meter reader: reads rtlamr output, parses readings, enqueues them.
Handles sleep/wake cycle and process restart on failure.
"""

import asyncio
import logging

import helpers.read_output as ro
import helpers.usb_utils as usbutil

logger = logging.getLogger('rtlamr2mqtt')

# If rtlamr produces no output for this many seconds while the process is still
# alive, declare it stuck and restart both rtlamr and rtl_tcp.
_STUCK_TIMEOUT = 600  # 10 minutes


class MeterReader:
    """
    Reads lines from the rtlamr ManagedProcess, parses meter readings,
    and puts them on the reading queue for the MQTT publisher.
    """

    def __init__(
        self,
        config: dict,
        rtlamr,
        rtltcp,
        reading_queue: asyncio.Queue,
        shutdown_event: asyncio.Event,
        is_remote: bool,
    ):
        self.config = config
        self.rtlamr = rtlamr
        self.rtltcp = rtltcp
        self.reading_queue = reading_queue
        self.shutdown_event = shutdown_event
        self.is_remote = is_remote
        self.meter_ids = list(config['meters'].keys())
        self.sleep_for = config['general']['sleep_for']
        self.rtltcp_host = config['general']['rtltcp_host']

    async def run(self):
        """
        Main reading loop. Reads from rtlamr, parses, enqueues.
        Handles sleep/wake cycle and process restarts.
        """
        while not self.shutdown_event.is_set():
            meters_seen = set()

            # Read until shutdown or all meters seen (when sleep_for > 0)
            while not self.shutdown_event.is_set():
                try:
                    line = await asyncio.wait_for(
                        self.rtlamr.read_line(),
                        timeout=_STUCK_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        'No output from rtlamr for %ds — process appears stuck, restarting',
                        _STUCK_TIMEOUT,
                    )
                    await self.rtlamr.stop()
                    if not self.is_remote:
                        await self.rtltcp.stop()
                        if not await self.rtltcp.start_with_retry():
                            logger.error('Failed to restart rtl_tcp after stuck rtlamr, shutting down')
                            self.shutdown_event.set()
                            return
                        await usbutil.tickle_rtl_tcp(self.rtltcp_host)
                        await asyncio.sleep(1.0)
                    if not await self.rtlamr.start_with_retry():
                        logger.error('Failed to restart rtlamr after stuck timeout, shutting down')
                        self.shutdown_event.set()
                        return
                    continue

                if line is None:
                    # Process died or stdout closed
                    if self.shutdown_event.is_set():
                        break
                    if not self.rtlamr.is_alive:
                        logger.warning('rtlamr process died, attempting restart')
                        # rtl_tcp exits when its client (rtlamr) disconnects.
                        # Restart rtl_tcp first if it also went down.
                        if not self.is_remote and not self.rtltcp.is_alive:
                            logger.warning('rtl_tcp also died, restarting both')
                            if not await self.rtltcp.start_with_retry():
                                logger.error('Failed to restart rtl_tcp, shutting down')
                                self.shutdown_event.set()
                                return
                            await usbutil.tickle_rtl_tcp(self.rtltcp_host)
                            await asyncio.sleep(1.0)
                        if not await self.rtlamr.start_with_retry():
                            logger.error('Failed to restart rtlamr, shutting down')
                            self.shutdown_event.set()
                            return
                    else:
                        # stdout closed but process alive — avoid tight loop
                        await asyncio.sleep(0.1)
                    continue

                if not line:
                    # Empty line
                    continue

                # Parse the line
                reading = ro.get_message_for_ids(line, self.meter_ids)
                if reading is None:
                    continue

                # Enqueue the reading
                try:
                    self.reading_queue.put_nowait(reading)
                except asyncio.QueueFull:
                    # Drop oldest reading to make room
                    try:
                        self.reading_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                    self.reading_queue.put_nowait(reading)
                    logger.warning('Reading queue full, dropped oldest reading')

                meters_seen.add(reading['meter_id'])
                logger.debug('Meter %s reading: %s', reading['meter_id'], reading['consumption'])

                # Check if all meters have been read (sleep_for mode)
                if self.sleep_for > 0 and meters_seen == set(self.meter_ids):
                    logger.info('All %d meters read', len(self.meter_ids))
                    break

            # Sleep/wake cycle
            if self.sleep_for > 0 and not self.shutdown_event.is_set():
                await self._sleep_cycle()

            # If sleep_for == 0 and we got here, the inner loop broke due to shutdown
            if self.sleep_for == 0:
                break

    async def _sleep_cycle(self):
        """
        Stop processes, sleep, restart processes.
        """
        logger.info('Stopping processes for sleep cycle')
        await self.rtlamr.stop()
        if not self.is_remote:
            await self.rtltcp.stop()

        logger.info('Sleeping for %d seconds', self.sleep_for)

        # Cancellable sleep
        sleep_task = asyncio.create_task(asyncio.sleep(self.sleep_for))
        shutdown_task = asyncio.create_task(self.shutdown_event.wait())
        done, pending = await asyncio.wait(
            [sleep_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if self.shutdown_event.is_set():
            return

        logger.info('Waking up, restarting processes')

        # The tickle can cause rtl_tcp to exit on some hardware. If rtlamr then
        # fails to connect, restart rtl_tcp and retry rather than giving up.
        _MAX_WAKE_ATTEMPTS = 3
        for attempt in range(1, _MAX_WAKE_ATTEMPTS + 1):
            logger.info('Wake attempt %d/%d', attempt, _MAX_WAKE_ATTEMPTS)

            # Restart rtl_tcp first (if local)
            if not self.is_remote:
                if not await self.rtltcp.start_with_retry():
                    logger.error('Failed to restart rtl_tcp after sleep, shutting down')
                    self.shutdown_event.set()
                    return
                logger.debug('rtl_tcp alive after start: %s', self.rtltcp.is_alive)

            # Tickle rtl_tcp to wake up the receiver
            logger.debug('Tickling rtl_tcp at %s', self.rtltcp_host)
            await usbutil.tickle_rtl_tcp(self.rtltcp_host)

            # Brief pause so rtl_tcp can die (if the tickle kills it) before we
            # check whether it is still alive and before rtlamr tries to connect.
            await asyncio.sleep(1.0)

            # If rtl_tcp exited after the tickle, loop back and restart it.
            if not self.is_remote:
                logger.debug('rtl_tcp alive after tickle+1s: %s', self.rtltcp.is_alive)
                if not self.rtltcp.is_alive:
                    logger.warning(
                        'rtl_tcp exited after tickle (attempt %d/%d), restarting...',
                        attempt, _MAX_WAKE_ATTEMPTS,
                    )
                    continue

            # Restart rtlamr
            logger.debug('Starting rtlamr (attempt %d/%d)', attempt, _MAX_WAKE_ATTEMPTS)
            if await self.rtlamr.start_with_retry():
                logger.info('Processes restarted successfully on wake attempt %d', attempt)
                return  # success — back to the main reading loop

            # rtlamr failed even though rtl_tcp looked alive.  Log rtl_tcp state
            # to help diagnose whether it crashed between the check and rtlamr's
            # first connection attempt, or whether rtlamr timed out for another reason.
            rtltcp_alive = self.rtltcp.is_alive if not self.is_remote else 'n/a (remote)'
            logger.warning(
                'rtlamr failed to start (attempt %d/%d); rtl_tcp alive=%s — restarting rtl_tcp...',
                attempt, _MAX_WAKE_ATTEMPTS, rtltcp_alive,
            )
            if not self.is_remote:
                await self.rtltcp.stop()

        logger.error('Failed to restart processes after sleep, shutting down')
        self.shutdown_event.set()
