#!/usr/bin/env python3
"""Измеряет скорость скачивания: N последовательных запросов к адресу."""

import argparse
import email.utils
import http.client
import json
import ssl
import sys
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (Any, Callable, Dict, List, Optional, Sequence, TextIO,
                    Tuple)

CHUNK_SIZE = 64 * 1024
USER_AGENT = "speedtest-script/2.0"
MAX_REDIRECTS = 5
MAX_RETRY_WAIT = 60.0  # верхняя граница паузы между повторами, с
REDIRECT_CODES = {301, 302, 303, 307, 308}
# Символы, которые не нужно экранировать в пути и query (RFC 3986).
URL_SAFE = "/%:@!$&'()*+,;=-._~?"
# Единица скорости: множитель «байт/с -> единица», подпись, подпись в JSON.
UNITS: Dict[str, Tuple[float, str, str]] = {
    "mbit": (8 / 1_000_000, "Мбит/с", "Mbit/s"),
    "mbyte": (1 / 1_000_000, "МБ/с", "MB/s"),
}

Log = Callable[[str], None]
Sleep = Callable[[float], None]


class FetchError(Exception):
    """Неудачный запрос.

    retryable   — имеет ли смысл повторить (сетевая ошибка, 429, 5xx);
    retry_after — сколько ждать по заголовку Retry-After, если он был.
    """

    def __init__(self, message: str, retryable: bool,
                 retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.retry_after = retry_after


@dataclass
class Timing:
    """Тайминги одного запроса (с учётом редиректов), в секундах."""

    size: int = 0           # байт тела итогового ответа
    connect: float = 0.0    # DNS + TCP + TLS
    ttfb: float = 0.0       # от отправки запроса до заголовков ответа
    download: float = 0.0   # чтение тела ответа

    @property
    def total(self) -> float:
        return self.connect + self.ttfb + self.download


@dataclass
class RequestResult:
    number: int
    timing: Optional[Timing] = None  # None — запрос так и не удался
    error: Optional[str] = None
    retries: int = 0


@dataclass
class Summary:
    successful: int
    failed: int
    total_bytes: int
    avg_time: float
    avg_connect: float
    avg_ttfb: float
    avg_download: float
    speed_bps: float           # байт/с: объём / полное время запросов
    download_speed_bps: float  # байт/с: объём / время чтения тела


def describe(error: BaseException) -> str:
    text = str(error)
    return f"{type(error).__name__}: {text}" if text else type(error).__name__


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Retry-After: число секунд или HTTP-дата. None, если не разобрать."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        moment = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return max(0.0, (moment - datetime.now(timezone.utc)).total_seconds())


class Fetcher:
    """HTTP-клиент: раздельные тайминги этапов и опциональный keep-alive."""

    def __init__(self, timeout: float, keep_alive: bool) -> None:
        self.timeout = timeout
        self.keep_alive = keep_alive
        self.connections_opened = 0
        self._ssl_context = ssl.create_default_context()
        self._conns: Dict[Tuple[str, str, int],
                          http.client.HTTPConnection] = {}

    def close(self) -> None:
        for conn in self._conns.values():
            conn.close()
        self._conns.clear()

    def fetch(self, url: str) -> Timing:
        """Скачивает ответ целиком, следуя редиректам."""
        timing = Timing()
        try:
            for _ in range(MAX_REDIRECTS + 1):
                location = self._fetch_once(url, timing)
                if location is None:
                    return timing
                url = urllib.parse.urljoin(url, location)
            raise FetchError(
                f"слишком много редиректов (больше {MAX_REDIRECTS})",
                retryable=False)
        except ssl.SSLCertVerificationError as e:
            self.close()
            raise FetchError(f"ошибка сертификата: {e}",
                             retryable=False) from e
        except (OSError, http.client.HTTPException) as e:
            self.close()
            raise FetchError(f"ошибка сети — {describe(e)}",
                             retryable=True) from e
        finally:
            if not self.keep_alive:
                self.close()

    def _connection(
            self, parts: urllib.parse.SplitResult
    ) -> http.client.HTTPConnection:
        host = parts.hostname or ""
        https = parts.scheme == "https"
        port = parts.port or (443 if https else 80)
        key = (parts.scheme, host, port)
        conn = self._conns.get(key)
        if conn is None:
            if https:
                conn = http.client.HTTPSConnection(
                    host, port, timeout=self.timeout,
                    context=self._ssl_context)
            else:
                conn = http.client.HTTPConnection(
                    host, port, timeout=self.timeout)
            self._conns[key] = conn
        return conn

    def _headers(self) -> Dict[str, str]:
        return {
            "User-Agent": USER_AGENT,
            # Просим реальный файл, а не закэшированный ответ.
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            # Без сжатия: считаем именно байты файла.
            "Accept-Encoding": "identity",
            "Connection": "keep-alive" if self.keep_alive else "close",
        }

    def _send(self, conn: http.client.HTTPConnection, target: str,
              timing: Timing) -> http.client.HTTPResponse:
        start = time.perf_counter()
        if conn.sock is None:
            conn.connect()
            self.connections_opened += 1
            now = time.perf_counter()
            timing.connect += now - start
            start = now
        conn.request("GET", target, headers=self._headers())
        response = conn.getresponse()
        timing.ttfb += time.perf_counter() - start
        return response

    def _fetch_once(self, url: str, timing: Timing) -> Optional[str]:
        """Один HTTP-обмен. Возвращает Location при редиректе, иначе None."""
        parts = urllib.parse.urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise FetchError(f"неподдерживаемый адрес: {url}",
                             retryable=False)
        target = urllib.parse.quote(parts.path or "/", safe=URL_SAFE)
        if parts.query:
            target += "?" + urllib.parse.quote(parts.query, safe=URL_SAFE)

        conn = self._connection(parts)
        reused = conn.sock is not None
        try:
            response = self._send(conn, target, timing)
        except (OSError, http.client.HTTPException):
            if not reused:
                raise
            # Сервер мог закрыть простаивающее keep-alive соединение:
            # переподключаемся один раз, повтором это не считается.
            conn.close()
            response = self._send(conn, target, timing)

        status = response.status
        location = response.getheader("Location")
        if status in REDIRECT_CODES and location:
            start = time.perf_counter()
            response.read()
            timing.ttfb += time.perf_counter() - start
            return location
        if not 200 <= status < 300:
            response.read()  # дочитываем, чтобы соединение было пригодно
            raise FetchError(
                f"HTTP {status} {response.reason}",
                retryable=status == 429 or status >= 500,
                retry_after=parse_retry_after(
                    response.getheader("Retry-After")))

        start = time.perf_counter()
        received = 0
        while chunk := response.read(CHUNK_SIZE):
            received += len(chunk)
        timing.download += time.perf_counter() - start
        timing.size = received

        # http.client молча отдаёт укороченное тело, если сервер
        # оборвал соединение раньше времени, — сверяем сами.
        expected = response.getheader("Content-Length")
        if expected and expected.isdigit() and received != int(expected):
            raise FetchError(
                f"ответ оборван: получено {received} из {expected} байт",
                retryable=True)
        return None


def fetch_with_retries(fetcher: Fetcher, url: str, number: int,
                       retries: int, retry_delay: float, log: Log,
                       sleep: Sleep = time.sleep) -> RequestResult:
    """Запрос с повторами: экспоненциальная пауза или Retry-After."""
    result = RequestResult(number)
    while True:
        try:
            result.timing = fetcher.fetch(url)
            result.error = None
            return result
        except FetchError as e:
            result.error = str(e)
            if not e.retryable or result.retries >= retries:
                return result
            if e.retry_after is not None:
                delay = e.retry_after
            else:
                delay = retry_delay * 2 ** result.retries
            delay = min(delay, MAX_RETRY_WAIT)
            result.retries += 1
            log(f"  {e}; повтор {result.retries}/{retries} "
                f"через {delay:.1f} с")
            sleep(delay)


def run_benchmark(url: str, count: int, *, warmup: int = 0,
                  keep_alive: bool = False, retries: int = 2,
                  retry_delay: float = 1.0, timeout: float = 30.0,
                  log: Log = lambda message: None,
                  sleep: Sleep = time.sleep
                  ) -> Tuple[List[RequestResult], int]:
    """Прогрев + count замеров. Возвращает (результаты, число соединений)."""
    fetcher = Fetcher(timeout, keep_alive)
    results: List[RequestResult] = []
    try:
        for i in range(1, warmup + 1):
            result = fetch_with_retries(fetcher, url, i, retries,
                                        retry_delay, log, sleep)
            log(format_result("Прогрев", result))
        for i in range(1, count + 1):
            result = fetch_with_retries(fetcher, url, i, retries,
                                        retry_delay, log, sleep)
            results.append(result)
            log(format_result("Запрос ", result))
    finally:
        fetcher.close()
    return results, fetcher.connections_opened


def summarize(results: List[RequestResult]) -> Optional[Summary]:
    """Сводка по успешным запросам; None, если успешных нет."""
    timings = [r.timing for r in results if r.timing is not None]
    if not timings:
        return None
    n = len(timings)
    total_bytes = sum(t.size for t in timings)
    total_time = sum(t.total for t in timings)
    download_time = sum(t.download for t in timings)
    return Summary(
        successful=n,
        failed=len(results) - n,
        total_bytes=total_bytes,
        avg_time=total_time / n,
        avg_connect=sum(t.connect for t in timings) / n,
        avg_ttfb=sum(t.ttfb for t in timings) / n,
        avg_download=download_time / n,
        speed_bps=total_bytes / total_time if total_time > 0 else 0.0,
        download_speed_bps=(total_bytes / download_time
                            if download_time > 0 else 0.0),
    )


def format_bytes(n: int) -> str:
    """Человекочитаемый объём в десятичных единицах (1 КБ = 1000 Б)."""
    if n < 1000:
        return f"{n} Б"
    value = float(n)
    for unit in ("КБ", "МБ", "ГБ"):
        value /= 1000
        if value < 1000 or unit == "ГБ":
            break
    return f"{value:.2f} {unit}"


def format_result(label: str, result: RequestResult) -> str:
    prefix = f"{label} {result.number:>2}:"
    retries = f" (повторов: {result.retries})" if result.retries else ""
    if result.timing is None:
        return f"{prefix} ошибка — {result.error}{retries}"
    t = result.timing
    return (f"{prefix} {format_bytes(t.size):>10} за {t.total:.3f} с  "
            f"[соединение {t.connect:.3f} + ожидание {t.ttfb:.3f} + "
            f"скачивание {t.download:.3f}]{retries}")


def print_summary(summary: Summary, count: int, connections: int,
                  units: str, out: TextIO) -> None:
    factor, label, _ = UNITS[units]
    rows = [
        ("Успешных запросов:", f"{summary.successful} из {count}"),
        ("Открыто соединений:", str(connections)),
        ("Среднее время запроса:", f"{summary.avg_time:.3f} с"),
        ("  установка соединения:", f"{summary.avg_connect:.3f} с"),
        ("  ожидание ответа:", f"{summary.avg_ttfb:.3f} с"),
        ("  скачивание тела:", f"{summary.avg_download:.3f} с"),
        ("Всего скачано:", f"{format_bytes(summary.total_bytes)} "
                           f"({summary.total_bytes} байт)"),
        ("Скорость скачивания:",
         f"{summary.speed_bps * factor:.2f} {label}"),
        ("  только тело ответа:",
         f"{summary.download_speed_bps * factor:.2f} {label}"),
    ]
    print(file=out)
    for name, value in rows:
        print(f"{name:<25}{value}", file=out)


def build_json(args: argparse.Namespace, results: List[RequestResult],
               summary: Optional[Summary],
               connections: int) -> Dict[str, Any]:
    factor, _, unit = UNITS[args.units]
    requests = []
    for r in results:
        item: Dict[str, Any] = {"number": r.number,
                                "ok": r.timing is not None,
                                "retries": r.retries, "error": r.error}
        if r.timing is not None:
            item.update(bytes=r.timing.size,
                        connect_s=round(r.timing.connect, 6),
                        ttfb_s=round(r.timing.ttfb, 6),
                        download_s=round(r.timing.download, 6),
                        total_s=round(r.timing.total, 6))
        requests.append(item)
    data: Dict[str, Any] = {
        "url": args.url, "count": args.count, "warmup": args.warmup,
        "keep_alive": args.keep_alive, "retries": args.retries,
        "connections_opened": connections, "requests": requests,
        "summary": None,
    }
    if summary is not None:
        data["summary"] = {
            "successful": summary.successful,
            "failed": summary.failed,
            "total_bytes": summary.total_bytes,
            "avg_time_s": round(summary.avg_time, 6),
            "avg_connect_s": round(summary.avg_connect, 6),
            "avg_ttfb_s": round(summary.avg_ttfb, 6),
            "avg_download_s": round(summary.avg_download, 6),
            "speed": round(summary.speed_bps * factor, 4),
            "download_speed": round(summary.download_speed_bps * factor, 4),
            "speed_unit": unit,
        }
    return data


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Делает N последовательных запросов к адресу "
                    "и считает скорость скачивания."
    )
    parser.add_argument(
        "url", help="адрес для скачивания, например тяжёлая картинка"
    )
    parser.add_argument(
        "-n", "--count", type=int, default=10,
        help="число замеряемых запросов (по умолчанию 10)",
    )
    parser.add_argument(
        "-t", "--timeout", type=float, default=30.0,
        help="таймаут сокета в секундах (по умолчанию 30)",
    )
    parser.add_argument(
        "-w", "--warmup", type=int, default=0,
        help="прогревочных запросов, не входят в статистику "
             "(по умолчанию 0)",
    )
    parser.add_argument(
        "-k", "--keep-alive", action="store_true",
        help="переиспользовать одно соединение для всех запросов",
    )
    parser.add_argument(
        "-r", "--retries", type=int, default=2,
        help="повторов при сетевой ошибке, 429 и 5xx (по умолчанию 2)",
    )
    parser.add_argument(
        "--retry-delay", type=float, default=1.0,
        help="начальная пауза перед повтором в секундах, дальше "
             "удваивается (по умолчанию 1)",
    )
    parser.add_argument(
        "-u", "--units", choices=sorted(UNITS), default="mbit",
        help="единица скорости: mbit — Мбит/с, mbyte — МБ/с "
             "(по умолчанию mbit)",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="вывести результат в JSON (прогресс уходит в stderr)",
    )
    args = parser.parse_args(argv)

    parts = urllib.parse.urlsplit(args.url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        parser.error("адрес должен начинаться с http:// или https://")
    try:
        parts.port
    except ValueError:
        parser.error("в адресе неверный порт")
    if args.count < 1:
        parser.error("--count должен быть не меньше 1")
    if args.timeout <= 0:
        parser.error("--timeout должен быть больше 0")
    if args.warmup < 0:
        parser.error("--warmup не может быть отрицательным")
    if args.retries < 0:
        parser.error("--retries не может быть отрицательным")
    if args.retry_delay < 0:
        parser.error("--retry-delay не может быть отрицательным")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    # В JSON-режиме stdout занят только JSON, прогресс — в stderr.
    out = sys.stderr if args.json else sys.stdout

    def log(message: str) -> None:
        print(message, file=out, flush=True)

    log(f"Адрес: {args.url}")
    log(f"Запросов: {args.count}, прогревочных: {args.warmup}, "
        f"keep-alive: {'да' if args.keep_alive else 'нет'}, "
        f"повторов при ошибке: до {args.retries}\n")

    results, connections = run_benchmark(
        args.url, args.count, warmup=args.warmup,
        keep_alive=args.keep_alive, retries=args.retries,
        retry_delay=args.retry_delay, timeout=args.timeout, log=log)
    summary = summarize(results)

    if args.json:
        print(json.dumps(build_json(args, results, summary, connections),
                         indent=2))
    elif summary is not None:
        print_summary(summary, args.count, connections, args.units, out)

    if summary is None:
        print("\nНи один запрос не удался.", file=sys.stderr)
        return 1
    return 0 if summary.failed == 0 else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nПрервано.", file=sys.stderr)
        sys.exit(130)
