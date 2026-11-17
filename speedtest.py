#!/usr/bin/env python3
"""Измеряет скорость скачивания: N последовательных запросов к одному адресу."""

import argparse
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

CHUNK_SIZE = 64 * 1024
USER_AGENT = "speedtest-script/1.0"


def fetch(url: str, timeout: float) -> tuple[int, float]:
    """Скачивает ответ целиком. Возвращает (байт тела, секунд)."""
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Accept-Encoding": "identity",
        },
    )
    start = time.perf_counter()
    received = 0
    with urllib.request.urlopen(request, timeout=timeout) as response:
        while chunk := response.read(CHUNK_SIZE):
            received += len(chunk)
    return received, time.perf_counter() - start


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Делает N последовательных запросов к адресу и считает скорость скачивания."
    )
    parser.add_argument("url", help="адрес для скачивания, например тяжёлая картинка")
    parser.add_argument("-n", "--count", type=int, default=10, help="число запросов (по умолчанию 10)")
    parser.add_argument("-t", "--timeout", type=float, default=30.0, help="таймаут сокета в секундах (по умолчанию 30)")
    args = parser.parse_args()

    if urllib.parse.urlparse(args.url).scheme not in ("http", "https"):
        parser.error("адрес должен начинаться с http:// или https://")
    if args.count < 1:
        parser.error("--count должен быть не меньше 1")
    if args.timeout <= 0:
        parser.error("--timeout должен быть больше 0")
    return args


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)  # прогресс виден сразу, даже при перенаправлении вывода
    args = parse_args()
    total_bytes = 0
    total_time = 0.0

    print(f"Адрес: {args.url}")
    print(f"Запросов: {args.count}\n")

    for i in range(1, args.count + 1):
        try:
            size, elapsed = fetch(args.url, args.timeout)
        except urllib.error.HTTPError as e:
            print(f"Запрос {i}: сервер ответил {e.code} {e.reason}", file=sys.stderr)
            return 1
        except (urllib.error.URLError, OSError) as e:
            reason = getattr(e, "reason", e)
            print(f"Запрос {i}: ошибка сети — {reason}", file=sys.stderr)
            return 1

        total_bytes += size
        total_time += elapsed
        print(f"Запрос {i:>2}: {format_bytes(size):>12} за {elapsed:.3f} с")

    avg_time = total_time / args.count
    bytes_per_sec = total_bytes / total_time if total_time > 0 else 0.0
    mbit_per_sec = bytes_per_sec * 8 / 1_000_000
    mbyte_per_sec = bytes_per_sec / 1_000_000

    print()
    print(f"Среднее время запроса: {avg_time:.3f} с")
    print(f"Всего скачано:         {format_bytes(total_bytes)} ({total_bytes} байт)")
    print(f"Скорость скачивания:   {mbit_per_sec:.2f} Мбит/с ({mbyte_per_sec:.2f} МБ/с)")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nПрервано.", file=sys.stderr)
        sys.exit(130)
