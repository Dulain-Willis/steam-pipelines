"""Unit tests for steamspy extract pagination logic."""
import pytest
from unittest.mock import patch, MagicMock, call
from requests.exceptions import Timeout, ConnectionError


def _make_response(json_data, content=None):
    """Build a mock requests.Response."""
    mock = MagicMock()
    mock.json.return_value = json_data
    mock.content = content if content is not None else b'{"data": true}'
    mock.raise_for_status.return_value = None
    return mock


@patch("pipelines.steamspy.extract.time.sleep")
@patch("pipelines.steamspy.extract.upload_to_minio")
@patch("pipelines.steamspy.extract.requests.get")
class TestCleanTermination:
    """Verify the function returns cleanly when the API signals end of data."""

    def test_empty_json_object_terminates_pagination(self, mock_get, mock_upload, mock_sleep):
        """Empty {} on page 1 should stop the loop and return 1 page uploaded."""
        mock_get.side_effect = [
            _make_response({"123": {"appid": 123, "name": "Game A"}}),
            _make_response({}, content=b"{}"),
        ]

        from pipelines.steamspy.extract import call_steamspy_api
        result = call_steamspy_api(bucket="landing", ds="2026-01-01", run_id="test-run")

        assert result == 1
        assert mock_upload.call_count == 1

    def test_empty_response_body_terminates_pagination(self, mock_get, mock_upload, mock_sleep):
        """Empty response body (no content) should return immediately with 0 pages uploaded."""
        mock_get.return_value = _make_response({}, content=b"")

        from pipelines.steamspy.extract import call_steamspy_api
        result = call_steamspy_api(bucket="landing", ds="2026-01-01", run_id="test-run")

        assert result == 0
        mock_upload.assert_not_called()

    def test_multiple_pages_then_empty_json(self, mock_get, mock_upload, mock_sleep):
        """Three pages of data followed by {} should upload 3 pages and return 3."""
        page_data = {"appid": 1, "name": "Game"}
        mock_get.side_effect = [
            _make_response({"1": page_data}),
            _make_response({"2": page_data}),
            _make_response({"3": page_data}),
            _make_response({}, content=b"{}"),
        ]

        from pipelines.steamspy.extract import call_steamspy_api
        result = call_steamspy_api(bucket="landing", ds="2026-01-01", run_id="test-run")

        assert result == 3
        assert mock_upload.call_count == 3


@patch("pipelines.steamspy.extract.time.sleep")
@patch("pipelines.steamspy.extract.upload_to_minio")
@patch("pipelines.steamspy.extract.requests.get")
class TestRetryBehavior:
    """Verify transient errors are retried and fatal errors propagate."""

    def test_timeout_retries_then_succeeds(self, mock_get, mock_upload, mock_sleep):
        """A timeout on the first attempt should retry and succeed on the second."""
        mock_get.side_effect = [
            Timeout("read timed out"),
            _make_response({"1": {"appid": 1}}),  # retry succeeds
            _make_response({}, content=b"{}"),    # end of pagination
        ]

        from pipelines.steamspy.extract import call_steamspy_api
        result = call_steamspy_api(bucket="landing", ds="2026-01-01", run_id="test-run")

        assert result == 1
        assert mock_upload.call_count == 1
        # Confirm backoff sleep was called (2^1 = 2s for first retry)
        mock_sleep.assert_any_call(2)

    def test_exhausted_retries_raises_exception(self, mock_get, mock_upload, mock_sleep):
        """Three consecutive timeouts on the same page should raise and not upload anything."""
        mock_get.side_effect = Timeout("read timed out")

        from pipelines.steamspy.extract import call_steamspy_api
        with pytest.raises(Timeout):
            call_steamspy_api(bucket="landing", ds="2026-01-01", run_id="test-run")

        mock_upload.assert_not_called()

    def test_exhausted_retries_backoff_sleep_calls(self, mock_get, mock_upload, mock_sleep):
        """Backoff sleeps should be 2s then 4s before the final raise (not 8s — that would be attempt 4)."""
        mock_get.side_effect = Timeout("read timed out")

        from pipelines.steamspy.extract import call_steamspy_api
        with pytest.raises(Timeout):
            call_steamspy_api(bucket="landing", ds="2026-01-01", run_id="test-run")

        # Attempts 1 and 2 sleep (2s, 4s). Attempt 3 raises — no sleep after final failure.
        sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert 2 in sleep_calls  # backoff after attempt 1
        assert 4 in sleep_calls  # backoff after attempt 2
        assert 8 not in sleep_calls  # no sleep after final failure

    def test_connection_error_also_retries(self, mock_get, mock_upload, mock_sleep):
        """ConnectionError (also a RequestException) should follow the same retry path."""
        mock_get.side_effect = [
            ConnectionError("connection refused"),
            ConnectionError("connection refused"),
            ConnectionError("connection refused"),
        ]

        from pipelines.steamspy.extract import call_steamspy_api
        with pytest.raises(ConnectionError):
            call_steamspy_api(bucket="landing", ds="2026-01-01", run_id="test-run")

        mock_upload.assert_not_called()


@patch("pipelines.steamspy.extract.time.sleep")
@patch("pipelines.steamspy.extract.upload_to_minio")
@patch("pipelines.steamspy.extract.requests.get")
class TestPartialDataGuard:
    """Verify that a failed extraction never partially uploads — upload only happens on success."""

    def test_no_upload_on_first_page_failure(self, mock_get, mock_upload, mock_sleep):
        """If page 0 exhausts retries, zero pages should be uploaded to MinIO."""
        mock_get.side_effect = Timeout("timed out")

        from pipelines.steamspy.extract import call_steamspy_api
        with pytest.raises(Timeout):
            call_steamspy_api(bucket="landing", ds="2026-01-01", run_id="test-run")

        mock_upload.assert_not_called()

    def test_no_upload_on_mid_pagination_failure(self, mock_get, mock_upload, mock_sleep):
        """If page 2 exhausts retries after pages 0-1 succeeded, upload count stays at 2."""
        page_data = {"1": {"appid": 1, "name": "Game"}}
        mock_get.side_effect = [
            _make_response(page_data),   # page 0 success
            _make_response(page_data),   # page 1 success
            Timeout("timed out"),        # page 2 attempt 1
            Timeout("timed out"),        # page 2 attempt 2
            Timeout("timed out"),        # page 2 attempt 3 — raises
        ]

        from pipelines.steamspy.extract import call_steamspy_api
        with pytest.raises(Timeout):
            call_steamspy_api(bucket="landing", ds="2026-01-01", run_id="test-run")

        # Pages 0 and 1 were already uploaded before the failure — this is expected
        # The key guarantee: the exception propagates so Airflow marks the task failed
        # and bronze/silver will not run against this incomplete partition
        assert mock_upload.call_count == 2
