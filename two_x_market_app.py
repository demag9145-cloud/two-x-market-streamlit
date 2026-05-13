from __future__ import annotations

import csv
import json
import math
import sys
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
try:
    from tkinter import BOTH, END, LEFT, RIGHT, X, Y, Button, Checkbutton, Entry, Frame, IntVar, Label, StringVar, Text, Tk, ttk
except ImportError:
    BOTH = END = LEFT = RIGHT = X = Y = Button = Checkbutton = Entry = Frame = IntVar = Label = StringVar = Text = Tk = ttk = None


APP_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "two_x_market_records"
CACHE_DIR = DATA_DIR / "price_cache"
SIGNAL_LOG = DATA_DIR / "signals.csv"

SIGNAL_SYMBOLS = ("QQQ", "TLT")
STOCK_BUY_SYMBOL = "QLD"
BOND_BUY_SYMBOL = "TLT"
FETCH_START_DATE = date(1999, 1, 1)
SIGNAL_START_MONTH = "2025-01"
RAW_DISPLAY_START_MONTH = "2024-01"
SOURCE_YAHOO = "yahoo"
SOURCE_ALPHA = "alpha"
SOURCE_YAHOO_ALPHA_BACKUP = "yahoo_alpha_backup"
CACHE_REFRESH_SECONDS = 300
FETCH_EVENTS: dict[tuple[str, str], str] = {}
MONTH_ABBR = {
    "jan": "01",
    "feb": "02",
    "mar": "03",
    "apr": "04",
    "may": "05",
    "jun": "06",
    "jul": "07",
    "aug": "08",
    "sep": "09",
    "oct": "10",
    "nov": "11",
    "dec": "12",
}


@dataclass(frozen=True)
class MonthPrice:
    month: str
    adj_close: float


@dataclass(frozen=True)
class SignalRow:
    execute_date: str
    qqq_score: float
    tlt_score: float
    combo: str
    target: str
    changed: bool


@dataclass(frozen=True)
class PriceStats:
    symbol: str
    source: str
    first_month: str
    latest_month: str
    latest_price: float | None
    rows: int
    missing_months: tuple[str, ...]
    compare_result: str


@dataclass(frozen=True)
class WorkflowResult:
    rows: list[SignalRow]
    log_path: str
    source_summary: str
    used_sources: dict[str, str]
    warnings: list[str]
    stats: list[PriceStats]
    prices: dict[str, dict[str, float]]
    alpha_prices: dict[str, dict[str, float]]
    checks: list["CheckItem"]
    is_preview: bool = False


@dataclass(frozen=True)
class CheckItem:
    status: str
    item: str
    detail: str


def ensure_dirs() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def add_months(ym: str, offset: int) -> str:
    year, month = map(int, ym.split("-"))
    month += offset
    year += (month - 1) // 12
    month = (month - 1) % 12 + 1
    return f"{year:04d}-{month:02d}"


def observed_fixed_holiday(year: int, month: int, day: int) -> date:
    d = date(year, month, day)
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def nth_weekday(year: int, month: int, weekday: int, nth: int) -> date:
    d = date(year, month, 1)
    while d.weekday() != weekday:
        d += timedelta(days=1)
    return d + timedelta(days=7 * (nth - 1))


def last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    return d


def easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def market_holidays(year: int) -> set[date]:
    holidays = {
        observed_fixed_holiday(year, 1, 1),
        nth_weekday(year, 1, 0, 3),
        nth_weekday(year, 2, 0, 3),
        easter_sunday(year) - timedelta(days=2),
        last_weekday(year, 5, 0),
        observed_fixed_holiday(year, 6, 19),
        observed_fixed_holiday(year, 7, 4),
        nth_weekday(year, 9, 0, 1),
        nth_weekday(year, 11, 3, 4),
        observed_fixed_holiday(year, 12, 25),
    }
    return holidays


def is_market_open_day(d: date) -> bool:
    return d.weekday() < 5 and d not in market_holidays(d.year)


def first_trading_day_after_month(month: str) -> date:
    next_month = add_months(month, 1)
    year, month_no = map(int, next_month.split("-"))
    d = date(year, month_no, 1)
    while not is_market_open_day(d):
        d += timedelta(days=1)
    return d


def latest_completed_month(today: date | None = None) -> str:
    today = today or date.today()
    first_this_month = date(today.year, today.month, 1)
    last_prev_month = first_this_month - timedelta(days=1)
    return month_key(last_prev_month)


def latest_formal_execute_date(today: date | None = None) -> str:
    return first_trading_day_after_month(latest_completed_month(today)).isoformat()


def keep_formal_rows(rows: list[SignalRow], today: date | None = None) -> list[SignalRow]:
    latest_allowed = latest_formal_execute_date(today)
    return [row for row in rows if row.execute_date <= latest_allowed]


def filter_prices_through(prices: dict[str, float], cutoff_month: str) -> dict[str, float]:
    return {ym: px for ym, px in prices.items() if ym <= cutoff_month}


def iter_months(start_month: str, end_month: str) -> list[str]:
    months: list[str] = []
    current = start_month
    while current <= end_month:
        months.append(current)
        current = add_months(current, 1)
    return months


def cache_path(symbol: str, source: str = SOURCE_YAHOO) -> Path:
    safe_source = source.lower().replace(" ", "_")
    return CACHE_DIR / f"{safe_source}_{symbol.upper()}_monthly_adjusted.csv"


def cache_is_fresh(symbol: str, source: str = SOURCE_YAHOO) -> bool:
    path = cache_path(symbol, source)
    if not path.exists():
        return False
    age = time.time() - path.stat().st_mtime
    return age < CACHE_REFRESH_SECONDS


def normalize_cache_month(raw_month: str) -> str:
    text = (raw_month or "").strip()
    if text.startswith('="') and text.endswith('"'):
        text = text[2:-1]
    if len(text) >= 7 and text[4] == "-" and text[:4].isdigit() and text[5:7].isdigit():
        return text[:7]

    parts = text.replace("/", "-").split("-")
    if len(parts) == 2:
        a, b = parts[0].strip(), parts[1].strip()
        if a[:3].lower() in MONTH_ABBR and b.isdigit():
            yy = int(b)
            year = 1900 + yy if yy >= 90 else 2000 + yy
            return f"{year:04d}-{MONTH_ABBR[a[:3].lower()]}"
        if b[:3].lower() in MONTH_ABBR and a.isdigit():
            raise ValueError(f"Ambiguous Excel date display: {raw_month}")
    raise ValueError(f"Invalid month value: {raw_month}")


def excel_text_month(month: str) -> str:
    return f'="{month}"'


def read_cached_prices(symbol: str, source: str = SOURCE_YAHOO) -> dict[str, float]:
    path = cache_path(symbol, source)
    if not path.exists():
        return {}

    out: dict[str, float] = {}
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            try:
                out[normalize_cache_month(row["month"])] = float(row["adj_close"])
            except (KeyError, TypeError, ValueError):
                continue
    return out


def write_cached_prices(symbol: str, prices: dict[str, float], source: str = SOURCE_YAHOO) -> None:
    path = cache_path(symbol, source)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["month", "adj_close"])
        writer.writeheader()
        for ym in sorted(prices):
            writer.writerow({"month": excel_text_month(ym), "adj_close": f"{prices[ym]:.8f}"})


def yahoo_chart_url(symbol: str, start: date, end: date) -> str:
    params = {
        "period1": int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp()),
        "period2": int(datetime(end.year, end.month, end.day, tzinfo=timezone.utc).timestamp()),
        "interval": "1mo",
        "events": "div,splits",
        "includeAdjustedClose": "true",
    }
    return f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol.upper()}?{urllib.parse.urlencode(params)}"


def fetch_yahoo_monthly_adjusted(symbol: str, start: date, end: date, retries: int = 3) -> dict[str, float]:
    url = yahoo_chart_url(symbol, start, end)
    last_error: Exception | None = None

    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 two-x-market-local-tool",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=25) as resp:
                payload = json.loads(resp.read().decode("utf-8"))

            result = payload["chart"]["result"][0]
            timestamps = result.get("timestamp", [])
            adj_close = result["indicators"]["adjclose"][0]["adjclose"]

            prices: dict[str, float] = {}
            for ts, px in zip(timestamps, adj_close):
                if px is None:
                    continue
                dt = datetime.fromtimestamp(ts, timezone.utc).date()
                prices[month_key(dt)] = float(px)
            return prices
        except Exception as exc:
            last_error = exc
            time.sleep(1.5 * (attempt + 1))

    raise RuntimeError(f"{symbol} Yahoo fetch failed: {last_error}")


def alpha_vantage_url(symbol: str, api_key: str) -> str:
    params = {
        "function": "TIME_SERIES_MONTHLY_ADJUSTED",
        "symbol": symbol.upper(),
        "datatype": "csv",
        "apikey": api_key.strip(),
    }
    return f"https://www.alphavantage.co/query?{urllib.parse.urlencode(params)}"


def fetch_alpha_monthly_adjusted(symbol: str, api_key: str, retries: int = 3) -> dict[str, float]:
    if not api_key.strip():
        raise RuntimeError("Alpha Vantage source needs an API key.")

    url = alpha_vantage_url(symbol, api_key)
    last_error: Exception | None = None

    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 two-x-market-local-tool",
                    "Accept": "text/csv,application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=25) as resp:
                text = resp.read().decode("utf-8-sig")

            if text.lstrip().startswith("{"):
                try:
                    message = json.loads(text)
                except json.JSONDecodeError:
                    message = text[:300]
                raise RuntimeError(f"Alpha Vantage returned JSON: {message}")

            reader = csv.DictReader(text.splitlines())
            prices: dict[str, float] = {}
            for row in reader:
                raw_date = row.get("timestamp")
                raw_price = row.get("adjusted close")
                if not raw_date or not raw_price:
                    continue
                dt = datetime.strptime(raw_date, "%Y-%m-%d").date()
                prices[month_key(dt)] = float(raw_price.replace(",", ""))

            if not prices:
                raise RuntimeError("Alpha Vantage returned no monthly prices.")
            return prices
        except Exception as exc:
            last_error = exc
            time.sleep(1.5 * (attempt + 1))

    raise RuntimeError(f"{symbol} Alpha Vantage fetch failed: {last_error}")


def fetch_monthly_adjusted(symbol: str, source: str, start: date, end: date, api_key: str = "") -> dict[str, float]:
    if source == SOURCE_ALPHA:
        return fetch_alpha_monthly_adjusted(symbol, api_key)
    return fetch_yahoo_monthly_adjusted(symbol, start, end)


def update_prices(symbol: str, start_month: str, include_current_month: bool, source: str = SOURCE_YAHOO, api_key: str = "") -> dict[str, float]:
    cached = read_cached_prices(symbol, source)
    cutoff = month_key(date.today()) if include_current_month else latest_completed_month()
    if cutoff in cached and cache_is_fresh(symbol, source):
        FETCH_EVENTS[(symbol.upper(), source)] = "cache"
        return filter_prices_through(cached, cutoff)

    end_day = date.today() + timedelta(days=5)
    fetched = fetch_monthly_adjusted(symbol, source, FETCH_START_DATE, end_day, api_key)

    merged = filter_prices_through(cached, cutoff)
    for ym, px in fetched.items():
        if ym <= cutoff:
            merged[ym] = px

    write_cached_prices(symbol, merged, source)
    FETCH_EVENTS[(symbol.upper(), source)] = "online"
    return merged


def update_prices_with_backup(symbol: str, start_month: str, include_current_month: bool, api_key: str = "") -> tuple[dict[str, float], str, str | None]:
    try:
        return update_prices(symbol, start_month, include_current_month, SOURCE_YAHOO), SOURCE_YAHOO, None
    except Exception as exc:
        cutoff = month_key(date.today()) if include_current_month else latest_completed_month()
        cached = read_cached_prices(symbol, SOURCE_YAHOO)
        if cutoff in cached:
            FETCH_EVENTS[(symbol.upper(), SOURCE_YAHOO)] = "offline_cache"
            return (
                filter_prices_through(cached, cutoff),
                SOURCE_YAHOO,
                f"{symbol}: Yahoo 線上取價失敗，已改用本地快取資料計算。原因: {exc}",
            )
        if not api_key.strip():
            raise
        fallback_prices = update_prices(symbol, start_month, include_current_month, SOURCE_ALPHA, api_key)
        return fallback_prices, SOURCE_ALPHA, f"{symbol}: Yahoo 抓價失敗，已改用 Alpha Vantage 備援。原因: {exc}"


def momentum_score(prices: dict[str, float], decision_month: str) -> float | None:
    current_month = add_months(decision_month, -1)
    months = [current_month, add_months(decision_month, -2), add_months(decision_month, -4), add_months(decision_month, -7)]
    if any(m not in prices for m in months):
        return None

    current = prices[months[0]]
    return ((current / prices[months[1]] - 1) + (current / prices[months[2]] - 1) + (current / prices[months[3]] - 1)) / 3


def choose_target(qqq: float, tlt: float, previous: str | None) -> str:
    eps = 1e-12
    if qqq > tlt and tlt > 0:
        return STOCK_BUY_SYMBOL
    if tlt > qqq and qqq > 0:
        return BOND_BUY_SYMBOL
    if qqq < 0 and tlt < 0:
        return "CASH"
    if (math.isclose(qqq, 0, abs_tol=eps) and tlt < 0) or (math.isclose(tlt, 0, abs_tol=eps) and qqq < 0):
        return previous or "HOLD"
    if qqq > 0 and tlt <= 0:
        return STOCK_BUY_SYMBOL
    if tlt > 0 and qqq <= 0:
        return BOND_BUY_SYMBOL
    if math.isclose(qqq, tlt, abs_tol=eps) and qqq > 0:
        return STOCK_BUY_SYMBOL
    if math.isclose(qqq, 0, abs_tol=eps) and math.isclose(tlt, 0, abs_tol=eps):
        return "CASH"
    return "REVIEW"


def build_signals(qqq_prices: dict[str, float], tlt_prices: dict[str, float], start_month: str = SIGNAL_START_MONTH) -> list[SignalRow]:
    all_months = sorted(set(qqq_prices) & set(tlt_prices))
    rows: list[SignalRow] = []
    previous: str | None = None

    for base_month in all_months:
        decision_month = add_months(base_month, 1)
        qqq = momentum_score(qqq_prices, decision_month)
        tlt = momentum_score(tlt_prices, decision_month)
        if qqq is None or tlt is None:
            continue

        target = choose_target(qqq, tlt, previous)
        changed = bool(previous and target != previous and target not in {"HOLD", "REVIEW"})
        execute_date = first_trading_day_after_month(base_month).isoformat()
        combo = ("+" if qqq > 0 else "-") + ("+" if tlt > 0 else "-")
        if execute_date[:7] >= start_month:
            rows.append(SignalRow(execute_date, qqq, tlt, combo, target, changed))
        if target not in {"HOLD", "REVIEW"}:
            previous = target

    return rows


def write_signal_log(rows: list[SignalRow], source: str) -> None:
    with SIGNAL_LOG.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["source", "execute_date", "qqq_score", "tlt_score", "combo", "target", "changed"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "source": source,
                    "execute_date": row.execute_date,
                    "qqq_score": f"{row.qqq_score:.8%}",
                    "tlt_score": f"{row.tlt_score:.8%}",
                    "combo": row.combo,
                    "target": row.target,
                    "changed": "YES" if row.changed else "NO",
                }
            )


def parse_percent(value: str) -> float:
    return float(value.strip().replace("%", "")) / 100


def read_signal_log() -> list[SignalRow]:
    if not SIGNAL_LOG.exists():
        return []

    rows: list[SignalRow] = []
    with SIGNAL_LOG.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                rows.append(
                    SignalRow(
                        execute_date=row["execute_date"],
                        qqq_score=parse_percent(row["qqq_score"]),
                        tlt_score=parse_percent(row["tlt_score"]),
                        combo=row["combo"],
                        target=row["target"],
                        changed=row["changed"].strip().upper() in {"YES", "TRUE"},
                    )
                )
            except (KeyError, ValueError):
                continue
    return rows


def latest_price_context(rows: list[SignalRow]) -> tuple[str, list[str]]:
    if not rows:
        return "", []
    decision_month = rows[-1].execute_date[:7]
    return decision_month, [
        add_months(decision_month, -1),
        add_months(decision_month, -2),
        add_months(decision_month, -4),
        add_months(decision_month, -7),
    ]


def build_price_stats(symbol: str, source: str, prices: dict[str, float], compare_result: str = "") -> PriceStats:
    if not prices:
        return PriceStats(symbol, source, "", "", None, 0, tuple(), compare_result)

    first_month = min(prices)
    latest_month = max(prices)
    check_start = max(first_month, RAW_DISPLAY_START_MONTH)
    expected = iter_months(check_start, latest_month)
    missing = tuple(month for month in expected if month not in prices)
    return PriceStats(symbol, source, first_month, latest_month, prices.get(latest_month), len(prices), missing[:12], compare_result)


def compare_alpha_in_background(primary_rows: list[SignalRow], include_current_month: bool, api_key: str) -> tuple[list[str], dict[str, str], dict[str, dict[str, float]]]:
    if not api_key.strip():
        return [], {}, {}

    warnings: list[str] = []
    compare_results = {symbol: "正常" for symbol in SIGNAL_SYMBOLS}
    alpha_prices: dict[str, dict[str, float]] = {}
    try:
        alpha_prices = {
            symbol: update_prices(symbol, SIGNAL_START_MONTH, include_current_month, SOURCE_ALPHA, api_key)
            for symbol in SIGNAL_SYMBOLS
        }
    except Exception as exc:
        cutoff = month_key(date.today()) if include_current_month else latest_completed_month()
        alpha_prices = {
            symbol: filter_prices_through(read_cached_prices(symbol, SOURCE_ALPHA), cutoff)
            for symbol in SIGNAL_SYMBOLS
        }
        warnings.append(f"Alpha Vantage 線上比對失敗，已改用本地 Alpha 快取顯示可用月份：{exc}")
        compare_results = {symbol: "快取比對" if alpha_prices.get(symbol) else "比對失敗" for symbol in SIGNAL_SYMBOLS}

    alpha_rows = build_signals(alpha_prices.get("QQQ", {}), alpha_prices.get("TLT", {}), SIGNAL_START_MONTH)
    if not alpha_rows:
        msg = "資料不足"
        return warnings + ["Alpha Vantage 後台比對失敗：資料不足，無法產生訊號。"], {symbol: msg for symbol in SIGNAL_SYMBOLS}, alpha_prices

    primary_latest = primary_rows[-1]
    alpha_latest = alpha_rows[-1]
    if primary_latest.execute_date != alpha_latest.execute_date:
        warnings.append(
            f"Alpha Vantage 比對日期不同：Yahoo {primary_latest.execute_date}，Alpha {alpha_latest.execute_date}。"
        )
        return warnings, {symbol: "日期不同" for symbol in SIGNAL_SYMBOLS}, alpha_prices

    q_diff = alpha_latest.qqq_score - primary_latest.qqq_score
    t_diff = alpha_latest.tlt_score - primary_latest.tlt_score
    if alpha_latest.target != primary_latest.target:
        warnings.append(
            f"Alpha Vantage 訊號不同：Yahoo {primary_latest.target}，Alpha {alpha_latest.target}。"
        )
        compare_results = {symbol: "訊號不同" for symbol in SIGNAL_SYMBOLS}
    if abs(q_diff) > 0.005 or abs(t_diff) > 0.005:
        warnings.append(
            "Alpha Vantage 分數差異較大："
            f"QQQ {q_diff:+.2%}，TLT {t_diff:+.2%}。"
        )
        if abs(q_diff) > 0.005:
            compare_results["QQQ"] = "分數差異"
        if abs(t_diff) > 0.005:
            compare_results["TLT"] = "分數差異"
    return warnings, compare_results, alpha_prices


def build_price_compare_text(symbol: str, month: str, yahoo_prices: dict[str, float], alpha_prices: dict[str, dict[str, float]]) -> str:
    if not alpha_prices:
        return ""
    y_price = yahoo_prices.get(month)
    a_price = alpha_prices.get(symbol, {}).get(month)
    if y_price is None:
        return "Yahoo缺資料"
    if a_price is None:
        return "Alpha缺資料"
    if y_price == 0:
        return "無法比對"
    diff = (a_price - y_price) / y_price
    threshold = price_compare_threshold(symbol, yahoo_prices, alpha_prices)
    if abs(diff) <= threshold:
        return f"正常 {diff:+.2%}"
    return f"差異 {diff:+.2%}>{threshold:.2%}"


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * pct)))
    return ordered[idx]


def price_compare_threshold(symbol: str, yahoo_prices: dict[str, float], alpha_prices: dict[str, dict[str, float]]) -> float:
    alpha_symbol_prices = alpha_prices.get(symbol, {})
    diffs: list[float] = []
    for month, y_price in yahoo_prices.items():
        if month < RAW_DISPLAY_START_MONTH:
            continue
        a_price = alpha_symbol_prices.get(month)
        if y_price and a_price:
            diffs.append(abs((a_price - y_price) / y_price))
    if len(diffs) < 6:
        return 0.005
    return max(0.005, percentile(diffs, 0.95) * 1.2)


def run_workflow(start_month: str, include_current_month: bool, source: str = SOURCE_YAHOO_ALPHA_BACKUP, api_key: str = "") -> WorkflowResult:
    ensure_dirs()
    FETCH_EVENTS.clear()
    is_preview = include_current_month
    prices: dict[str, dict[str, float]] = {}
    used_sources: dict[str, str] = {}
    warnings: list[str] = []
    checks: list[CheckItem] = []
    for symbol in SIGNAL_SYMBOLS:
        if source == SOURCE_YAHOO_ALPHA_BACKUP:
            symbol_prices, used_source, warning = update_prices_with_backup(symbol, start_month, include_current_month, api_key)
            if warning:
                warnings.append(warning)
        else:
            symbol_prices = update_prices(symbol, start_month, include_current_month, source, api_key)
            used_source = source
        prices[symbol] = symbol_prices
        used_sources[symbol] = used_source
        fetch_mode = FETCH_EVENTS.get((symbol.upper(), used_source), "online")
        if fetch_mode == "cache":
            fetch_detail = "使用本地表格/快取資料計算，未線上重新取價。"
            fetch_status = "SKIP"
        elif fetch_mode == "offline_cache":
            fetch_detail = "Yahoo 線上取價失敗，改用本地快取資料計算。"
            fetch_status = "SKIP"
        else:
            fetch_detail = "已線上重新取價並更新本地快取。"
            fetch_status = "OK"
        checks.append(CheckItem("OK", f"{symbol} Yahoo 抓價", f"來源使用 {used_source}，最新月 {max(symbol_prices) if symbol_prices else '無'}"))
        checks.append(CheckItem(fetch_status, f"{symbol} 取價方式", fetch_detail))

    rows = build_signals(prices["QQQ"], prices["TLT"], start_month)
    if not rows:
        raise RuntimeError("資料不足，無法計算訊號。請把起始月份往前調，或稍後再更新。")
    checks.append(CheckItem("OK", "動能公式計算", f"已產生 {len(rows)} 筆訊號。"))
    source_summary = "/".join(f"{symbol}:{used_sources[symbol]}" for symbol in SIGNAL_SYMBOLS)
    if is_preview:
        checks.append(CheckItem("SKIP", "正式紀錄寫入", "月底預估模式不寫入正式 signals.csv。"))
    else:
        write_signal_log(rows, source_summary)
        checks.append(CheckItem("OK", "紀錄寫入", str(SIGNAL_LOG)))
    compare_results: dict[str, str] = {}
    alpha_prices: dict[str, dict[str, float]] = {}
    if all(used_sources.get(symbol) == SOURCE_YAHOO for symbol in SIGNAL_SYMBOLS):
        compare_warnings, compare_results, alpha_prices = compare_alpha_in_background(rows, include_current_month, api_key)
        warnings.extend(compare_warnings)
        if api_key.strip():
            status = "OK" if not compare_warnings else "FAIL"
            threshold_bits = []
            for symbol in SIGNAL_SYMBOLS:
                threshold = price_compare_threshold(symbol, prices[symbol], alpha_prices)
                threshold_bits.append(f"{symbol}容忍{threshold:.2%}")
            detail = "Alpha Vantage 後台比對正常（" + "，".join(threshold_bits) + "）。" if not compare_warnings else "；".join(compare_warnings)
            checks.append(CheckItem(status, "Alpha 後台比對", detail))
        else:
            checks.append(CheckItem("SKIP", "Alpha 後台比對", "未輸入 API key，略過比對。"))

    stats = [
        build_price_stats(symbol, used_sources[symbol], prices[symbol], compare_results.get(symbol, ""))
        for symbol in SIGNAL_SYMBOLS
    ]
    expected_latest = month_key(date.today()) if is_preview else latest_completed_month()
    for stat in stats:
        if stat.latest_month and stat.latest_month < expected_latest:
            warnings.append(f"{stat.symbol}: 最新資料只到 {stat.latest_month}，目標最新月為 {expected_latest}。")
            checks.append(CheckItem("FAIL", f"{stat.symbol} 最新月", f"最新 {stat.latest_month}，目標 {expected_latest}。"))
        else:
            latest_label = "最新預估月" if is_preview else "最新完整月"
            checks.append(CheckItem("OK", f"{stat.symbol} 最新月", f"{latest_label} {stat.latest_month}。"))
        if stat.missing_months:
            warnings.append(f"{stat.symbol}: {RAW_DISPLAY_START_MONTH} 以後缺月份 {', '.join(stat.missing_months)}。")
            checks.append(CheckItem("FAIL", f"{stat.symbol} 缺月份", ", ".join(stat.missing_months)))
        else:
            checks.append(CheckItem("OK", f"{stat.symbol} 缺月份", f"{RAW_DISPLAY_START_MONTH} 以後未偵測缺月份。"))
    log_path = "月底預估模式未寫入正式紀錄" if is_preview else str(SIGNAL_LOG)
    return WorkflowResult(rows, log_path, source_summary, used_sources, warnings, stats, prices, alpha_prices, checks, is_preview)


class TwoXMarketApp:
    def __init__(self, root: Tk) -> None:
        self.root = root
        self.root.title("2倍大盤動能計算")
        self._set_initial_window()

        self.status = StringVar(value="尚未更新")
        self.signal_date = StringVar(value="尚未更新")
        self.signal_target = StringVar(value="--")
        self.signal_change = StringVar(value="--")
        self.signal_scores = StringVar(value="按一鍵更新後會抓價、計算並寫入紀錄。")
        self.detail = StringVar(value="完整月模式：本月未收盤時，只用到上個月月底月 K。")
        self.include_current = IntVar(value=0)
        self.alpha_key = StringVar(value="")
        self.warning_text = StringVar(value="尚無異常提醒。")
        self.signal_change_label: Label | None = None
        self.current_prices: dict[str, dict[str, float]] = {}
        self.current_alpha_prices: dict[str, dict[str, float]] = {}
        self.current_sources: dict[str, str] = {}

        self._configure_style()
        self._build_ui()
        self.load_existing_records()

    def _set_initial_window(self) -> None:
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        width = min(1680, max(1480, screen_w - 80))
        height = min(960, max(860, screen_h - 100))
        x = max(0, (screen_w - width) // 2)
        y = max(0, (screen_h - height) // 2)
        self.root.geometry(f"{width}x{height}+{x}+{y}")
        self.root.minsize(1360, 780)
        if screen_w <= 1600 or screen_h <= 900:
            try:
                self.root.state("zoomed")
            except Exception:
                pass

    def _configure_style(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Treeview", rowheight=26, font=("Microsoft JhengHei UI", 10))
        style.configure("Treeview.Heading", font=("Microsoft JhengHei UI", 10, "bold"))

    def _section(self, parent: Frame, title: str) -> tuple[Frame, Frame]:
        outer = Frame(parent, bg="#f3f6f8", highlightthickness=1, highlightbackground="#d8e0e6")
        Label(outer, text=title, bg="#f3f6f8", fg="#213547", font=("Microsoft JhengHei UI", 11, "bold"), anchor="w").pack(fill=X, padx=12, pady=(10, 4))
        inner = Frame(outer, bg="#ffffff")
        inner.pack(fill=BOTH, expand=True, padx=10, pady=(0, 10))
        return outer, inner

    def _build_ui(self) -> None:
        self.root.configure(bg="#eef3f6")

        top = Frame(self.root, padx=18, pady=14, bg="#17384d")
        top.pack(fill=X)

        title = Label(top, text="2倍大盤動能計算", bg="#17384d", fg="#ffffff", font=("Microsoft JhengHei UI", 18, "bold"))
        title.pack(side=LEFT)

        Button(top, text="使用說明", command=self.show_help, width=10).pack(side=RIGHT, padx=(8, 0))
        Button(top, text="首頁", command=self.show_home, width=8).pack(side=RIGHT, padx=(8, 0))
        Button(top, text="月底預估", command=lambda: self.update_async(True), width=12).pack(side=RIGHT, padx=(8, 0))
        Button(top, text="正式計算", command=lambda: self.update_async(False), width=12).pack(side=RIGHT)

        self.home_page = Frame(self.root, bg="#eef3f6")
        self.home_page.pack(fill=BOTH, expand=True)

        options = Frame(self.home_page, padx=18, pady=10, bg="#eef3f6")
        options.pack(fill=X)
        Label(options, text="資料來源：Yahoo", bg="#eef3f6", fg="#213547", font=("Microsoft JhengHei UI", 10, "bold")).pack(side=LEFT)
        Label(options, text="Alpha Key(備援/後台比對)", bg="#eef3f6").pack(side=LEFT, padx=(18, 0))
        Entry(options, textvariable=self.alpha_key, width=22, show="*").pack(side=LEFT, padx=(6, 14))
        Label(options, text="正式計算只用完成月 K；月底預估用本月目前價格", bg="#eef3f6", fg="#2563eb", font=("Microsoft JhengHei UI", 10, "bold")).pack(side=LEFT)

        main = Frame(self.home_page, padx=18, pady=14, bg="#eef3f6")
        main.pack(fill=BOTH, expand=True)

        top_grid = Frame(main, bg="#eef3f6")
        top_grid.pack(fill=X)

        signal_outer, signal_box = self._section(top_grid, "最新訊號")
        signal_outer.pack(side=LEFT, fill=BOTH, expand=True, padx=(0, 8))
        signal_head = Frame(signal_box, bg="#ffffff")
        signal_head.pack(fill=X, padx=12, pady=(10, 4))
        Label(signal_head, textvariable=self.signal_date, bg="#ffffff", fg="#486581", font=("Microsoft JhengHei UI", 13, "bold"), anchor="w").pack(side=LEFT)
        Label(signal_head, text="建議標的：", bg="#ffffff", fg="#102a43", font=("Microsoft JhengHei UI", 17, "bold")).pack(side=LEFT, padx=(16, 0))
        Label(signal_head, textvariable=self.signal_target, bg="#ffffff", fg="#0b6b3a", font=("Microsoft JhengHei UI", 24, "bold")).pack(side=LEFT)
        self.signal_change_label = Label(signal_head, textvariable=self.signal_change, bg="#dbeafe", fg="#1d4ed8", font=("Microsoft JhengHei UI", 20, "bold"), padx=12, pady=3)
        self.signal_change_label.pack(side=LEFT, padx=(18, 0))
        Label(signal_box, textvariable=self.signal_scores, bg="#ffffff", fg="#102a43", font=("Microsoft JhengHei UI", 13, "bold"), anchor="w", justify=LEFT).pack(fill=X, padx=12, pady=(2, 5))
        Label(signal_box, textvariable=self.detail, bg="#ffffff", fg="#486581", font=("Microsoft JhengHei UI", 10), anchor="w", justify=LEFT).pack(fill=X, padx=12, pady=(0, 10))
        Label(signal_box, textvariable=self.status, bg="#ffffff", fg="#627d98", anchor="w").pack(fill=X, padx=12, pady=(0, 10))

        warning_outer, warning_box = self._section(top_grid, "檢查項目")
        warning_outer.pack(side=RIGHT, fill=BOTH, expand=True, padx=(8, 0))
        self.check_table = ttk.Treeview(warning_box, columns=("status", "item", "detail"), show="headings", height=5)
        for col, label, width in (
            ("status", "狀態", 62),
            ("item", "項目", 130),
            ("detail", "說明", 360),
        ):
            self.check_table.heading(col, text=label)
            self.check_table.column(col, width=width, anchor="center" if col == "status" else "w")
        self.check_table.tag_configure("ok", background="#dcfce7")
        self.check_table.tag_configure("fail", background="#fecaca")
        self.check_table.tag_configure("skip", background="#fde68a")
        check_scroll = ttk.Scrollbar(warning_box, orient="vertical", command=self.check_table.yview)
        self.check_table.configure(yscrollcommand=check_scroll.set)
        self.check_table.pack(side=LEFT, fill=BOTH, expand=True, padx=(8, 0), pady=8)
        check_scroll.pack(side=RIGHT, fill="y", padx=(0, 8), pady=8)

        middle = Frame(main, bg="#eef3f6")
        middle.pack(fill=BOTH, expand=True, pady=(12, 0))

        stats_outer, stats_box = self._section(middle, "資料檢查")
        stats_outer.pack(side=LEFT, fill=BOTH, padx=(0, 8), expand=False)
        self.stats_table = ttk.Treeview(stats_box, columns=("symbol", "source", "first", "latest", "price", "rows", "compare", "missing"), show="headings", height=4)
        for col, label, width in (
            ("symbol", "標的", 70),
            ("source", "來源", 90),
            ("first", "最早月", 90),
            ("latest", "最新月", 90),
            ("price", "最新月K", 90),
            ("rows", "筆數", 70),
            ("compare", "比對結果", 90),
            ("missing", "缺月份", 180),
        ):
            self.stats_table.heading(col, text=label)
            self.stats_table.column(col, width=width, anchor="center")
        self.stats_table.pack(fill=BOTH, expand=True, padx=8, pady=8)

        Label(stats_box, text="本次計算用價", bg="#ffffff", fg="#213547", font=("Microsoft JhengHei UI", 10, "bold"), anchor="w").pack(fill=X, padx=8, pady=(4, 0))
        self.price_table = ttk.Treeview(stats_box, columns=("symbol", "role", "month", "price", "source", "compare"), show="headings", height=8)
        for col, label, width in (
            ("symbol", "標的", 64),
            ("role", "用途", 88),
            ("month", "月份", 84),
            ("price", "Adj Close", 96),
            ("source", "來源", 80),
            ("compare", "Alpha比對", 116),
        ):
            self.price_table.heading(col, text=label)
            self.price_table.column(col, width=width, anchor="center")
        self.price_table.tag_configure("base", background="#fef3c7")
        self.price_table.tag_configure("compare_bad", background="#fecaca")
        self.price_table.pack(fill=BOTH, expand=True, padx=8, pady=(4, 8))

        table_outer, table_frame = self._section(middle, "動能歷史紀錄")
        table_outer.pack(side=RIGHT, fill=BOTH, expand=True, padx=(8, 0))

        columns = ("date", "qqq", "tlt", "combo", "target", "changed")
        self.table = ttk.Treeview(table_frame, columns=columns, show="headings", height=16)
        headings = {
            "date": "執行日",
            "qqq": "QQQ動能",
            "tlt": "TLT動能",
            "combo": "組合",
            "target": "標的",
            "changed": "換倉",
        }
        widths = {"date": 110, "qqq": 120, "tlt": 120, "combo": 80, "target": 90, "changed": 80}
        for col in columns:
            self.table.heading(col, text=headings[col])
            self.table.column(col, width=widths[col], anchor="center")
        self.table.tag_configure("changed", background="#ffe4e6")
        self.table.pack(fill=BOTH, expand=True, padx=8, pady=8)

        self.help_page = Frame(self.root, padx=26, pady=18, bg="#eef3f6")
        self._build_help_page(self.help_page)

    def _build_help_page(self, parent: Frame) -> None:
        Label(parent, text="使用說明", bg="#eef3f6", fg="#17384d", font=("Microsoft JhengHei UI", 20, "bold"), anchor="w").pack(fill=X)
        Label(
            parent,
            text="本頁說明按鈕、資料來源、欄位與異常提醒。看完可按上方「首頁」回到計算畫面。",
            bg="#eef3f6",
            fg="#486581",
            font=("Microsoft JhengHei UI", 11),
            anchor="w",
        ).pack(fill=X, pady=(4, 12))

        text_frame = Frame(parent, bg="#ffffff", highlightthickness=1, highlightbackground="#d8e0e6")
        text_frame.pack(fill=BOTH, expand=True)
        help_text = Text(
            text_frame,
            wrap="word",
            bg="#ffffff",
            fg="#102a43",
            font=("Microsoft JhengHei UI", 11),
            padx=18,
            pady=14,
            relief="flat",
        )
        scroll = ttk.Scrollbar(text_frame, orient="vertical", command=help_text.yview)
        help_text.configure(yscrollcommand=scroll.set)
        help_text.pack(side=LEFT, fill=BOTH, expand=True)
        scroll.pack(side=RIGHT, fill=Y)
        content = self.help_text()
        help_text.insert(END, content)
        cache_tip = "重要：正式計算與月底預估都有 5 分鐘快取保護。"
        start = content.find(cache_tip)
        if start >= 0:
            line_no = content[:start].count("\n") + 1
            help_text.tag_add("cache_tip", f"{line_no}.0", f"{line_no}.end")
            help_text.tag_configure("cache_tip", foreground="#9a3412", background="#fed7aa", font=("Microsoft JhengHei UI", 11, "bold"))
        help_text.configure(state="disabled")

    def help_text(self) -> str:
        return """【這個程式在做什麼】
本工具用 QQQ 與 TLT 的調整後月收盤價，計算 1 個月、3 個月、6 個月平均動能，產生本月建議標的與是否換倉。

公式邏輯：
以基準月價格分別對比 1 個月前、3 個月前、6 個月前價格，三個報酬率平均後得到動能分數。

例如正式計算 2026-05-01：
基準月是 2026-04，對比 2026-03、2026-01、2025-10。


【上方按鈕】
正式計算
只使用已完成的月 K。結果會寫入正式紀錄 signals.csv，適合每月正式檢查與換倉。

月底預估
用本月目前價格暫代本月月底月 K，提前預估下一個執行月的建議標的。畫面會顯示「月底預估 YYYY-MM-DD」，但不寫入正式紀錄。

首頁
回到主計算畫面。

使用說明
切換到本頁。


【API Key】
目前主資料來源是 Yahoo，不需要 API Key。

Alpha Key 是選填，用途有兩個：
1. Yahoo 抓價異常時，可作為備援來源。
2. 有輸入 Alpha Key 時，程式會在後台比對 Yahoo 與 Alpha Vantage 的價格差異。

沒有輸入 Alpha Key：
程式只使用 Yahoo，不做 Alpha 後台比對，檢查項目會顯示略過。

有輸入 Alpha Key：
程式會顯示比對是否正常。若差異超過門檻，會在檢查項目與計算用價欄位標示異常。


【取價與快取】
重要：正式計算與月底預估都有 5 分鐘快取保護。

第一次按下正式計算或月底預估時，若本地沒有本次需要的月份價格，程式會線上抓價並寫入本地快取。

如果 5 分鐘內重複按同一種需要相同月份資料的計算，程式會直接使用本地表格/快取資料，不會線上重新取價。檢查項目會顯示「使用本地表格/快取資料計算，未線上重新取價」。

超過 5 分鐘後再按，程式會重新嘗試線上取價並更新快取。


【最新訊號】
這裡是最重要的結果區。

日期
正式計算會顯示正式執行日，例如 2026-05-01。
月底預估會顯示「月底預估 2026-06-01」這類文字，提醒這不是正式紀錄。

建議標的
QLD：偏向持有 2 倍 Nasdaq 方向。
TLT：偏向債券方向。
CASH：偏向現金。

需要換倉 / 不用換倉
與上一筆正式或預估連續結果比較後，判斷標的是否改變。

計算結果
顯示 QQQ 與 TLT 的動能分數。

計算月份
顯示本次使用的基準月，以及對比的 1 個月、3 個月、6 個月價格月份。


【檢查項目】
綠色勾選：檢查通過。
黃色提示：不是錯誤，通常是未輸入 Alpha Key 所以略過比對，或預估模式不寫入正式紀錄。
紅色異常：需要注意，可能是 Yahoo 抓價失敗、Alpha 比對差異過大、缺月份或資料不足。


【資料檢查】
標的
目前固定檢查 QQQ 與 TLT。

來源
正常情況顯示 yahoo；若 Yahoo 失敗且有 Alpha Key，可能改用 alpha。

最早月 / 最新月
目前資料涵蓋的月份範圍。

最新月K
最新月份的調整後收盤價。

筆數
目前保留的月資料筆數。

比對結果
有輸入 Alpha Key 才會顯示後台比對結果。

缺月份
若 2024-01 之後資料有缺月，會在這裡列出。


【本次計算用價】
列出本次公式實際用到的價格：
基準月、1 個月、3 個月、6 個月。

基準月會用不同底色標示，方便交叉比對 Yahoo 奇摩或其他資料來源。

Alpha 比對欄位：
空白：未輸入 Alpha Key，沒有比對。
正常 +0.xx%：兩邊價格差異在可接受範圍。
差異 +x.xx%：差異超過門檻，該列會變紅色底色，請人工確認 Yahoo / Alpha 價格。
Alpha缺資料：Alpha 沒有該月份價格，該列會變紅色底色，請人工確認。
Yahoo缺資料：Yahoo 沒有該月份價格，該列會變紅色底色，不能直接採用。

只要本次計算用價的 Alpha 比對欄位不是空白或正常，程式會用紅色底色提示；正式採用前請人工核對。


【動能歷史紀錄】
顯示最近兩年的正式動能紀錄。

換倉 = YES
代表本月建議標的和上一筆不同，該列會用底色標示。

換倉 = NO
代表標的未改變，維持原本配置。


【建議使用流程】
每月 1 號或月初：
按「正式計算」，確認最新訊號、檢查項目與計算用價，再決定是否換倉。

月底前幾天想先觀察：
按「月底預估」，提前看下一個月可能的建議標的。預估結果只供參考，不會寫入正式紀錄。

如果出現紅色異常：
先看檢查項目的說明，再檢查資料檢查與本次計算用價。如果有 Alpha Key，可輸入後再跑一次比對。
"""

    def show_home(self) -> None:
        self.help_page.pack_forget()
        self.home_page.pack(fill=BOTH, expand=True)

    def show_help(self) -> None:
        self.home_page.pack_forget()
        self.help_page.pack(fill=BOTH, expand=True)

    def load_existing_records(self) -> None:
        rows = keep_formal_rows(read_signal_log())
        if not rows:
            return
        self.current_prices = {
            symbol: filter_prices_through(read_cached_prices(symbol, SOURCE_YAHOO), latest_completed_month())
            for symbol in SIGNAL_SYMBOLS
        }
        self.current_alpha_prices = {}
        self.current_sources = {symbol: SOURCE_YAHOO for symbol in SIGNAL_SYMBOLS}
        latest = rows[-1]
        self.signal_date.set(latest.execute_date)
        self.signal_target.set(latest.target)
        self.signal_change.set("需要換倉" if latest.changed else "不用換倉")
        self.apply_signal_change_style(latest.changed)
        self.signal_scores.set(f"上次計算結果｜QQQ {latest.qqq_score:.2%} / TLT {latest.tlt_score:.2%}")
        _, months = latest_price_context(rows)
        if months:
            self.detail.set(f"計算月份：{months[0]} 對比 {months[1]} / {months[2]} / {months[3]}")
        self.status.set(f"已載入 {SIGNAL_LOG}")
        self.render_cached_stats()
        self.render_price_usage(rows)
        self.render_history(rows)
        self.render_checks([CheckItem("SKIP", "Alpha 後台比對", "啟動載入既有紀錄，按一鍵更新後執行完整檢查。")])

    def update_async(self, preview: bool = False) -> None:
        mode_text = "月底預估" if preview else "正式計算"
        self.status.set(f"{mode_text}中：正在用 Yahoo 抓取調整後月收盤價...")
        self.render_checks([CheckItem("SKIP", mode_text, "正在檢查資料來源、缺月份與後台比對。")])
        threading.Thread(target=self._update, args=(preview,), daemon=True).start()

    def _update(self, preview: bool = False) -> None:
        try:
            result = run_workflow(SIGNAL_START_MONTH, preview, SOURCE_YAHOO_ALPHA_BACKUP, self.alpha_key.get())
            self.root.after(0, lambda: self.render(result))
        except Exception as exc:
            self.root.after(0, lambda: self.status.set(str(exc)))

    def render(self, result: WorkflowResult) -> None:
        rows = result.rows
        latest = rows[-1]
        changed_text = "需要換倉" if latest.changed else "不用換倉"
        _, months = latest_price_context(rows)
        self.current_prices = result.prices
        self.current_alpha_prices = result.alpha_prices
        self.current_sources = result.used_sources
        self.signal_date.set(latest.execute_date)
        self.signal_target.set(latest.target)
        self.signal_change.set(changed_text)
        self.apply_signal_change_style(latest.changed)
        prefix = "預估計算結果" if result.is_preview else "計算結果"
        self.signal_scores.set(f"{prefix}｜QQQ {latest.qqq_score:.2%} / TLT {latest.tlt_score:.2%}")
        month_label = "預估基準月" if result.is_preview else "計算月份"
        self.detail.set(f"來源：{result.source_summary}｜{month_label}：{months[0]} 對比 {months[1]} / {months[2]} / {months[3]}")
        status_prefix = "預估完成" if result.is_preview else "完成"
        self.status.set(f"{status_prefix}，{result.log_path}")
        if result.warnings:
            pass
        else:
            pass
        self.render_checks(result.checks)

        self.stats_table.delete(*self.stats_table.get_children())
        for stat in result.stats:
            missing = ", ".join(stat.missing_months) if stat.missing_months else "-"
            latest_price = "" if stat.latest_price is None else f"{stat.latest_price:.4f}"
            self.stats_table.insert(
                "",
                END,
                values=(stat.symbol, stat.source, stat.first_month, stat.latest_month, latest_price, stat.rows, stat.compare_result, missing),
            )

        self.render_price_usage(rows)
        self.render_history(rows)

    def apply_signal_change_style(self, changed: bool) -> None:
        if self.signal_change_label is None:
            return
        if changed:
            self.signal_change_label.configure(bg="#fed7aa", fg="#9a3412")
        else:
            self.signal_change_label.configure(bg="#dbeafe", fg="#1d4ed8")

    def render_cached_stats(self) -> None:
        self.stats_table.delete(*self.stats_table.get_children())
        for symbol in SIGNAL_SYMBOLS:
            prices = self.current_prices.get(symbol, {})
            stat = build_price_stats(symbol, self.current_sources.get(symbol, SOURCE_YAHOO), prices, "")
            missing = ", ".join(stat.missing_months) if stat.missing_months else "-"
            latest_price = "" if stat.latest_price is None else f"{stat.latest_price:.4f}"
            self.stats_table.insert(
                "",
                END,
                values=(stat.symbol, stat.source, stat.first_month, stat.latest_month, latest_price, stat.rows, stat.compare_result, missing),
            )

    def render_price_usage(self, rows: list[SignalRow]) -> None:
        self.price_table.delete(*self.price_table.get_children())
        _, months = latest_price_context(rows)
        roles = ["基準月", "1個月", "3個月", "6個月"]
        if not months:
            return
        for symbol in SIGNAL_SYMBOLS:
            prices = self.current_prices.get(symbol, {})
            source = self.current_sources.get(symbol, SOURCE_YAHOO)
            for role, month in zip(roles, months):
                price = prices.get(month)
                compare = build_price_compare_text(symbol, month, prices, self.current_alpha_prices)
                tags: list[str] = []
                if role == "基準月":
                    tags.append("base")
                if compare.startswith("差異") or "缺" in compare:
                    tags.append("compare_bad")
                self.price_table.insert(
                    "",
                    END,
                    values=(
                        symbol,
                        role,
                        month,
                        "" if price is None else f"{price:.4f}",
                        source,
                        compare,
                    ),
                    tags=tuple(tags),
                )

    def render_checks(self, checks: list[CheckItem]) -> None:
        self.check_table.delete(*self.check_table.get_children())
        icon_map = {"OK": "✓", "FAIL": "✕", "SKIP": "－"}
        tag_map = {"OK": "ok", "FAIL": "fail", "SKIP": "skip"}
        for check in checks:
            self.check_table.insert(
                "",
                END,
                values=(icon_map.get(check.status, check.status), check.item, check.detail),
                tags=(tag_map.get(check.status, "skip"),),
            )

    def render_history(self, rows: list[SignalRow]) -> None:
        self.table.delete(*self.table.get_children())
        for row in rows[-24:]:
            tags: list[str] = []
            if row.changed:
                tags.append("changed")
            self.table.insert(
                "",
                END,
                values=(
                    row.execute_date,
                    f"{row.qqq_score:.2%}",
                    f"{row.tlt_score:.2%}",
                    row.combo,
                    row.target,
                    "YES" if row.changed else "NO",
                ),
                tags=tuple(tags),
            )


def main() -> None:
    root = Tk()
    TwoXMarketApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
