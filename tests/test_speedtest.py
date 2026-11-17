"""Тесты speedtest.py на локальном HTTP-сервере (без внешней сети)."""

import http.server
import json
import threading
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest

import speedtest

SIZE = 200_000  # байт в «файле»


class Handler(http.server.BaseHTTPRequestHandler):
    """Поведение задаётся путём запроса."""

    protocol_version = "HTTP/1.1"  # нужен для keep-alive

    def log_message(self, *args):
        pass

    def setup(self):
        super().setup()
        self.server.connections += 1  # одно TCP-соединение

    def do_GET(self):
        hits = self.server.hits
        hits[self.path] = hits.get(self.path, 0) + 1
        n = hits[self.path]

        if self.path == "/file":
            self._send(200, b"x" * SIZE)
        elif self.path == "/flaky":  # первые два раза — 503
            if n <= 2:
                self._send(503, b"busy")
            else:
                self._send(200, b"x" * SIZE)
        elif self.path == "/ratelimit":  # первый раз — 429 + Retry-After
            if n == 1:
                self._send(429, b"slow down", {"Retry-After": "0"})
            else:
                self._send(200, b"x" * SIZE)
        elif self.path == "/missing":
            self._send(404, b"not found")
        elif self.path == "/redirect":
            self._send(302, b"", {"Location": "/file"})
        elif self.path == "/loop":
            self._send(302, b"", {"Location": "/loop"})
        elif self.path == "/truncated":  # обещаем SIZE, отдаём половину
            self.send_response(200)
            self.send_header("Content-Length", str(SIZE))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(b"x" * (SIZE // 2))
            self.close_connection = True
        else:
            self._send(500, b"unknown path")

    def _send(self, status, body, headers=None):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def server():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    srv.connections = 0
    srv.hits = {}
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield srv
    srv.shutdown()
    srv.server_close()


def url(server, path):
    return f"http://127.0.0.1:{server.server_address[1]}{path}"


def run(server, path, count=1, **kwargs):
    """run_benchmark без реальных пауз; паузы пишутся в список sleeps."""
    sleeps = []
    kwargs.setdefault("retry_delay", 0.5)
    results, connections = speedtest.run_benchmark(
        url(server, path), count, sleep=sleeps.append, **kwargs)
    return results, connections, sleeps


# --- Скачивание и тайминги -------------------------------------------------

def test_downloads_full_body_every_time(server):
    results, _, _ = run(server, "/file", count=3)
    assert [r.timing.size for r in results] == [SIZE] * 3
    assert all(r.error is None for r in results)
    assert server.hits["/file"] == 3


def test_timing_parts_are_measured(server):
    results, _, _ = run(server, "/file")
    t = results[0].timing
    assert t.connect > 0 and t.ttfb > 0 and t.download >= 0
    assert t.total == pytest.approx(t.connect + t.ttfb + t.download)


def test_new_connection_per_request_by_default(server):
    _, connections, _ = run(server, "/file", count=4)
    assert connections == 4
    assert server.connections == 4


def test_keep_alive_reuses_one_connection(server):
    results, connections, _ = run(server, "/file", count=4,
                                  keep_alive=True)
    assert connections == 1
    assert server.connections == 1
    # Соединение устанавливается только в первом запросе.
    assert results[0].timing.connect > 0
    assert all(r.timing.connect == 0 for r in results[1:])


def test_warmup_requests_are_not_in_results(server):
    results, _, _ = run(server, "/file", count=3, warmup=2)
    assert [r.number for r in results] == [1, 2, 3]
    assert server.hits["/file"] == 5


def test_redirect_is_followed(server):
    results, _, _ = run(server, "/redirect")
    assert results[0].timing.size == SIZE
    assert server.hits["/file"] == 1


def test_redirect_loop_is_an_error(server):
    results, _, _ = run(server, "/loop")
    assert results[0].timing is None
    assert "редирект" in results[0].error


# --- Ошибки и повторы ------------------------------------------------------

def test_5xx_is_retried_with_exponential_backoff(server):
    results, _, sleeps = run(server, "/flaky", retries=2)
    assert results[0].timing.size == SIZE
    assert results[0].retries == 2
    assert results[0].error is None
    assert sleeps == [0.5, 1.0]


def test_gives_up_after_retries_are_exhausted(server):
    results, _, sleeps = run(server, "/flaky", retries=1)
    assert results[0].timing is None
    assert "503" in results[0].error
    assert results[0].retries == 1
    assert len(sleeps) == 1


def test_retry_after_header_overrides_backoff(server):
    results, _, sleeps = run(server, "/ratelimit", retry_delay=5)
    assert results[0].timing is not None
    assert sleeps == [0.0]


def test_4xx_is_not_retried(server):
    results, _, sleeps = run(server, "/missing", retries=3)
    assert "404" in results[0].error
    assert results[0].retries == 0
    assert sleeps == []
    assert server.hits["/missing"] == 1


def test_failed_request_does_not_stop_the_run(server):
    results, _, _ = run(server, "/flaky", count=3, retries=0)
    assert [r.timing is not None for r in results] == [False, False, True]


def test_truncated_body_is_detected(server):
    results, _, _ = run(server, "/truncated", retries=0)
    assert results[0].timing is None
    assert "оборван" in results[0].error


def test_connection_refused_is_a_network_error(server):
    port = server.server_address[1]
    server.shutdown()
    server.server_close()
    results, _ = speedtest.run_benchmark(
        f"http://127.0.0.1:{port}/file", 1, retries=0, timeout=2)
    assert "ошибка сети" in results[0].error


# --- Разбор Retry-After ----------------------------------------------------

@pytest.mark.parametrize("value, expected", [
    (None, None),
    ("", None),
    ("7", 7.0),
    (" 3 ", 3.0),
    ("не дата", None),
    ("Wed, 21 Oct 2015 07:28:00 GMT", 0.0),  # дата в прошлом
])
def test_parse_retry_after(value, expected):
    assert speedtest.parse_retry_after(value) == expected


def test_parse_retry_after_future_date():
    moment = datetime.now(timezone.utc) + timedelta(seconds=30)
    seconds = speedtest.parse_retry_after(format_datetime(moment, True))
    assert 25 < seconds <= 30


# --- Сводка и форматирование ----------------------------------------------

def test_summarize_math():
    results = [
        speedtest.RequestResult(1, speedtest.Timing(1_000_000, 0.5, 0.5, 1)),
        speedtest.RequestResult(2, speedtest.Timing(1_000_000, 0, 0, 2)),
        speedtest.RequestResult(3, error="HTTP 503"),
    ]
    s = speedtest.summarize(results)
    assert (s.successful, s.failed, s.total_bytes) == (2, 1, 2_000_000)
    assert s.avg_time == pytest.approx(2.0)
    assert s.avg_connect == pytest.approx(0.25)
    assert s.speed_bps == pytest.approx(500_000)           # 2 МБ / 4 с
    assert s.download_speed_bps == pytest.approx(666_666.67)  # 2 МБ / 3 с


def test_summarize_without_successes():
    assert speedtest.summarize([speedtest.RequestResult(1, error="x")]) \
        is None


@pytest.mark.parametrize("n, expected", [
    (0, "0 Б"),
    (999, "999 Б"),
    (1000, "1.00 КБ"),
    (10_485_760, "10.49 МБ"),
    (2_500_000_000, "2.50 ГБ"),
])
def test_format_bytes(n, expected):
    assert speedtest.format_bytes(n) == expected


# --- CLI -------------------------------------------------------------------

def test_main_text_output(server, capsys):
    code = speedtest.main([url(server, "/file"), "-n", "2"])
    out = capsys.readouterr().out
    assert code == 0
    assert "Успешных запросов:       2 из 2" in out
    assert "Мбит/с" in out


def test_main_json_output(server, capsys):
    code = speedtest.main([url(server, "/file"), "-n", "2", "-k",
                           "--units", "mbyte", "--json"])
    captured = capsys.readouterr()
    data = json.loads(captured.out)  # stdout — только JSON
    assert code == 0
    assert data["connections_opened"] == 1
    assert data["summary"]["successful"] == 2
    assert data["summary"]["total_bytes"] == 2 * SIZE
    assert data["summary"]["speed_unit"] == "MB/s"
    assert [r["bytes"] for r in data["requests"]] == [SIZE, SIZE]
    assert "Запрос" in captured.err  # прогресс ушёл в stderr


def test_main_exit_code_when_requests_fail(server, capsys):
    code = speedtest.main([url(server, "/missing"), "-n", "1"])
    assert code == 1
    assert "Ни один запрос не удался" in capsys.readouterr().err


@pytest.mark.parametrize("argv", [
    ["ftp://example.com/file"],
    ["http://example.com:99999/"],
    ["http://example.com/", "-n", "0"],
    ["http://example.com/", "-r", "-1"],
    ["http://example.com/", "--units", "kbit"],
])
def test_main_rejects_bad_arguments(argv):
    with pytest.raises(SystemExit) as exc:
        speedtest.main(argv)
    assert exc.value.code == 2
