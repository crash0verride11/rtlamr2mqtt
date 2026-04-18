import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from meter_reader import MeterReader


@pytest.fixture
def mock_rtlamr():
    proc = AsyncMock()
    proc.is_alive = True
    proc.start_with_retry = AsyncMock(return_value=True)
    proc.stop = AsyncMock()
    return proc


@pytest.fixture
def mock_rtltcp():
    proc = AsyncMock()
    proc.is_alive = True
    proc.start_with_retry = AsyncMock(return_value=True)
    proc.stop = AsyncMock()
    return proc


@pytest.fixture
def reader(sample_config, mock_rtlamr, mock_rtltcp):
    queue = asyncio.Queue()
    shutdown = asyncio.Event()
    return MeterReader(
        config=sample_config,
        rtlamr=mock_rtlamr,
        rtltcp=mock_rtltcp,
        reading_queue=queue,
        shutdown_event=shutdown,
        is_remote=False,
    )


class TestMeterReaderParsing:
    async def test_valid_reading_enqueued(self, reader, mock_rtlamr, sample_rtlamr_scm_line):
        """A valid SCM reading for a configured meter should be put on the queue."""
        call_count = 0
        async def fake_read_line():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return sample_rtlamr_scm_line
            reader.shutdown_event.set()
            return None
        mock_rtlamr.read_line = fake_read_line

        await reader.run()

        assert reader.reading_queue.qsize() == 1
        reading = await reader.reading_queue.get()
        assert reading['meter_id'] == '33333333'
        assert reading['consumption'] == 1978226

    async def test_non_matching_id_not_enqueued(self, reader, mock_rtlamr):
        """Lines for meters not in config should be ignored."""
        non_matching_line = '{"Time":"2025-05-05T21:25:10Z","Offset":0,"Length":0,"Type":"R900","Message":{"ID":9999999,"Unkn1":163,"NoUse":0,"BackFlow":0,"Consumption":100,"Unkn3":0,"Leak":0,"LeakNow":0}}'
        call_count = 0
        async def fake_read_line():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return non_matching_line
            reader.shutdown_event.set()
            return None
        mock_rtlamr.read_line = fake_read_line

        await reader.run()
        assert reader.reading_queue.qsize() == 0

    async def test_non_json_line_ignored(self, reader, mock_rtlamr):
        """Non-JSON lines (rtlamr debug output) should be silently ignored."""
        call_count = 0
        async def fake_read_line():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return 'set freq 912600155'
            reader.shutdown_event.set()
            return None
        mock_rtlamr.read_line = fake_read_line

        await reader.run()
        assert reader.reading_queue.qsize() == 0


class TestMeterReaderSleepCycle:
    async def test_sleep_cycle_when_all_meters_read(self, sample_config, mock_rtlamr, mock_rtltcp):
        """When sleep_for > 0 and all meters read, processes should be stopped then restarted."""
        sample_config['general']['sleep_for'] = 1
        queue = asyncio.Queue()
        shutdown = asyncio.Event()
        reader = MeterReader(
            config=sample_config,
            rtlamr=mock_rtlamr,
            rtltcp=mock_rtltcp,
            reading_queue=queue,
            shutdown_event=shutdown,
            is_remote=False,
        )

        scm_33 = '{"Time":"2025-05-05T21:25:11Z","Offset":0,"Length":0,"Type":"SCM","Message":{"ID":33333333,"Type":7,"TamperPhy":3,"TamperEnc":2,"Consumption":1978226,"ChecksumVal":60151}}'
        scm_22 = '{"Time":"2025-05-05T21:25:11Z","Offset":0,"Length":0,"Type":"SCM","Message":{"ID":22222222,"Type":7,"TamperPhy":0,"TamperEnc":1,"Consumption":9480653,"ChecksumVal":8042}}'

        call_count = 0
        cycle_count = 0
        async def fake_read_line():
            nonlocal call_count, cycle_count
            call_count += 1
            if cycle_count == 0:
                if call_count == 1:
                    return scm_33
                if call_count == 2:
                    return scm_22
            cycle_count += 1
            shutdown.set()
            return None
        mock_rtlamr.read_line = fake_read_line

        await reader.run()

        # Both processes should have been stopped for sleep
        assert mock_rtlamr.stop.call_count >= 1
        assert mock_rtltcp.stop.call_count >= 1
        # Two readings should be on the queue
        assert queue.qsize() == 2


class TestMeterReaderProcessRestart:
        self, sample_config, mock_rtlamr, mock_rtltcp
    ):
        """
        If rtl_tcp dies after the tickle and rtlamr cannot connect, the sleep
        cycle should restart rtl_tcp and retry rtlamr rather than shutting down.
        """
        sample_config['general']['sleep_for'] = 1
        queue = asyncio.Queue()
        shutdown = asyncio.Event()

        # rtl_tcp is alive on first start_with_retry, then appears dead after tickle,
        # then alive again on the second start_with_retry.
        rtltcp_start_count = 0
        async def rtltcp_start_with_retry():
            nonlocal rtltcp_start_count
            rtltcp_start_count += 1
            return True
        mock_rtltcp.start_with_retry = rtltcp_start_with_retry

        # is_alive: False on the first check (post-tickle), True after restart.
        is_alive_calls = [False, True]
        is_alive_idx = 0
        def get_is_alive():
            nonlocal is_alive_idx
            val = is_alive_calls[min(is_alive_idx, len(is_alive_calls) - 1)]
            is_alive_idx += 1
            return val
        type(mock_rtltcp).is_alive = property(lambda self: get_is_alive())

        # rtlamr succeeds on the second wake attempt (after rtl_tcp is restarted)
        rtlamr_start_count = 0
        async def rtlamr_start_with_retry():
            nonlocal rtlamr_start_count
            rtlamr_start_count += 1
            return True
        mock_rtlamr.start_with_retry = rtlamr_start_with_retry

        scm_33 = '{"Time":"2025-05-05T21:25:11Z","Offset":0,"Length":0,"Type":"SCM","Message":{"ID":33333333,"Type":7,"TamperPhy":3,"TamperEnc":2,"Consumption":1978226,"ChecksumVal":60151}}'
        scm_22 = '{"Time":"2025-05-05T21:25:11Z","Offset":0,"Length":0,"Type":"SCM","Message":{"ID":22222222,"Type":7,"TamperPhy":0,"TamperEnc":1,"Consumption":9480653,"ChecksumVal":8042}}'

        call_count = 0
        cycle_done = False
        async def fake_read_line():
            nonlocal call_count, cycle_done
            call_count += 1
            if not cycle_done:
                if call_count == 1:
                    return scm_33
                if call_count == 2:
                    cycle_done = True
                    return scm_22
            shutdown.set()
            return None
        mock_rtlamr.read_line = fake_read_line

        reader = MeterReader(
            config=sample_config,
            rtlamr=mock_rtlamr,
            rtltcp=mock_rtltcp,
            reading_queue=queue,
            shutdown_event=shutdown,
            is_remote=False,
        )

        with patch('meter_reader.usbutil.tickle_rtl_tcp'):
            await reader.run()

        # rtl_tcp should have been started twice (once for the failed tickle
        # attempt, once for the successful retry).
        assert rtltcp_start_count >= 2
        # rtlamr should have started successfully after the retry.
        assert rtlamr_start_count >= 1
        assert not shutdown.is_set()

    async def test_sleep_cycle_shuts_down_when_all_wake_attempts_exhausted(
        self, sample_config, mock_rtlamr, mock_rtltcp
    ):
        """
        If rtl_tcp keeps dying after the tickle across all retry attempts,
        shutdown_event should be set.
        """
        sample_config['general']['sleep_for'] = 1
        queue = asyncio.Queue()
        shutdown = asyncio.Event()

        mock_rtltcp.start_with_retry = AsyncMock(return_value=True)
        # rtl_tcp always appears dead after the tickle.
        type(mock_rtltcp).is_alive = property(lambda self: False)

        scm_33 = '{"Time":"2025-05-05T21:25:11Z","Offset":0,"Length":0,"Type":"SCM","Message":{"ID":33333333,"Type":7,"TamperPhy":3,"TamperEnc":2,"Consumption":1978226,"ChecksumVal":60151}}'
        scm_22 = '{"Time":"2025-05-05T21:25:11Z","Offset":0,"Length":0,"Type":"SCM","Message":{"ID":22222222,"Type":7,"TamperPhy":0,"TamperEnc":1,"Consumption":9480653,"ChecksumVal":8042}}'

        call_count = 0
        async def fake_read_line():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return scm_33
            return scm_22
        mock_rtlamr.read_line = fake_read_line

        reader = MeterReader(
            config=sample_config,
            rtlamr=mock_rtlamr,
            rtltcp=mock_rtltcp,
            reading_queue=queue,
            shutdown_event=shutdown,
            is_remote=False,
        )

        with patch('meter_reader.usbutil.tickle_rtl_tcp'):
            await reader.run()

        assert shutdown.is_set()


class TestMeterReaderProcessRestart:
    async def test_restart_on_rtlamr_death(self, reader, mock_rtlamr):
        """If rtlamr dies (read_line returns None), it should be restarted."""
        call_count = 0
        async def fake_read_line():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return None  # Simulate process death
            reader.shutdown_event.set()
            return None
        mock_rtlamr.read_line = fake_read_line
        mock_rtlamr.is_alive = False

        await reader.run()

        # Should have attempted to restart rtlamr
        assert mock_rtlamr.start_with_retry.call_count >= 1

    async def test_shutdown_on_failed_restart(self, reader, mock_rtlamr):
        """If rtlamr can't restart, shutdown_event should be set."""
        mock_rtlamr.read_line = AsyncMock(return_value=None)
        mock_rtlamr.is_alive = False
        mock_rtlamr.start_with_retry = AsyncMock(return_value=False)

        await reader.run()

        assert reader.shutdown_event.is_set()
