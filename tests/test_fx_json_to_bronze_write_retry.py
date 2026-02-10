import pytest

from project_a.jobs.fx_json_to_bronze import (
    _is_concurrent_append_exception,
    _normalize_run_date,
    _write_delta_run_slice_with_retry,
)


class _FakeWriter:
    def __init__(self, failures_before_success: int, error_text: str = ""):
        self.failures_before_success = failures_before_success
        self.error_text = error_text
        self.save_calls = 0
        self.replace_where = None

    def format(self, _fmt):
        return self

    def mode(self, _mode):
        return self

    def option(self, key, value):
        if key == "replaceWhere":
            self.replace_where = value
        return self

    def partitionBy(self, *_cols):
        return self

    def save(self, _path):
        self.save_calls += 1
        if self.save_calls <= self.failures_before_success:
            raise Exception(self.error_text)


class _FakeDataFrame:
    def __init__(self, writer: _FakeWriter):
        self.write = writer


def test_normalize_run_date_valid():
    assert _normalize_run_date("2026-02-10") == "2026-02-10"


def test_normalize_run_date_invalid():
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        _normalize_run_date("2026/02/10")


def test_retry_on_concurrent_append(monkeypatch):
    writer = _FakeWriter(
        failures_before_success=2,
        error_text="io.delta.exceptions.ConcurrentAppendException: concurrent update",
    )
    df = _FakeDataFrame(writer)
    sleeps = []
    monkeypatch.setattr("project_a.jobs.fx_json_to_bronze.time.sleep", lambda s: sleeps.append(s))

    _write_delta_run_slice_with_retry(
        df=df,
        output_path="data/bronze/fx/delta",
        run_date="2026-02-10",
        max_attempts=5,
        base_backoff_seconds=0.01,
    )

    assert writer.save_calls == 3
    assert writer.replace_where == "_run_date = DATE '2026-02-10'"
    assert len(sleeps) == 2


def test_no_retry_for_non_concurrency_error(monkeypatch):
    writer = _FakeWriter(failures_before_success=1, error_text="Some other write failure")
    df = _FakeDataFrame(writer)
    sleeps = []
    monkeypatch.setattr("project_a.jobs.fx_json_to_bronze.time.sleep", lambda s: sleeps.append(s))

    with pytest.raises(Exception, match="Some other write failure"):
        _write_delta_run_slice_with_retry(
            df=df,
            output_path="data/bronze/fx/delta",
            run_date="2026-02-10",
            max_attempts=5,
            base_backoff_seconds=0.01,
        )

    assert writer.save_calls == 1
    assert sleeps == []


def test_concurrent_append_detector():
    assert _is_concurrent_append_exception(Exception("ConcurrentAppendException"))
    assert _is_concurrent_append_exception(Exception("files were added by a concurrent update"))
    assert not _is_concurrent_append_exception(Exception("schema mismatch"))
