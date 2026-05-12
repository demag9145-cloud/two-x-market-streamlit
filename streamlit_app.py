import os
from dataclasses import asdict

import pandas as pd
import streamlit as st

import two_x_market_app as core


st.set_page_config(
    page_title="2倍大盤動能計算",
    page_icon="2X",
    layout="wide",
    initial_sidebar_state="collapsed",
)


st.markdown(
    """
    <style>
    .stApp { background: #eef3f6; color: #102a43; }
    [data-testid="stSidebar"] { display: none; }
    .block-container { padding: .75rem 1.15rem 1.1rem 1.15rem; max-width: 100%; }
    .topbar {
        background: #17384d;
        color: white;
        padding: .8rem 1rem;
        border-radius: 0;
        margin: -.75rem -1.15rem .7rem -1.15rem;
    }
    .top-title { font-size: 1.55rem; font-weight: 800; line-height: 1.25; }
    .option-row {
        background: #eef3f6;
        color: #213547;
        padding: .15rem 0 .55rem 0;
        font-size: .9rem;
    }
    .section-title {
        color: #213547;
        font-size: 1rem;
        font-weight: 800;
        margin: .15rem 0 .35rem 0;
    }
    .signal-box {
        background: #ffffff;
        border: 1px solid #d8e0e6;
        border-radius: 4px;
        padding: .85rem .95rem;
        min-height: 170px;
    }
    .signal-date { color: #486581; font-size: 1rem; font-weight: 700; }
    .signal-target { color: #0b6b3a; font-size: 1.95rem; font-weight: 800; }
    .signal-change {
        display: inline-block;
        padding: .25rem .65rem;
        border-radius: 2px;
        font-size: 1.35rem;
        font-weight: 800;
        margin-left: .4rem;
    }
    .change-yes { background: #fed7aa; color: #9a3412; }
    .change-no { background: #dbeafe; color: #1d4ed8; }
    .score-line { color: #102a43; font-size: 1.05rem; font-weight: 800; margin-top: .35rem; }
    .muted-line { color: #486581; font-size: .92rem; margin-top: .35rem; }
    div[data-testid="stDataFrame"] {
        border: 1px solid #d8e0e6;
        border-radius: 4px;
        background: #ffffff;
    }
    div.stButton > button {
        height: 2rem;
        border-radius: 2px;
        border: 1px solid #9aa6b2;
        background: #f8fafc;
        color: #102a43;
        font-weight: 700;
    }
    div.stButton > button:hover { border-color: #2563eb; color: #17384d; }
    h1, h2, h3 { color: #213547; }
    .help-box {
        background: #ffffff;
        border: 1px solid #d8e0e6;
        padding: 1rem 1.2rem;
        border-radius: 4px;
        color: #102a43;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def configured_password() -> str:
    try:
        return st.secrets.get("APP_PASSWORD", "")
    except Exception:
        return os.environ.get("APP_PASSWORD", "")


def require_password() -> bool:
    password = configured_password()
    if not password:
        return True
    if st.session_state.get("auth_ok"):
        return True
    st.title("2倍大盤動能計算")
    entered = st.text_input("請輸入密碼", type="password")
    if st.button("進入"):
        if entered == password:
            st.session_state.auth_ok = True
            st.rerun()
        st.error("密碼錯誤")
    return False


def signal_to_dict(row: core.SignalRow) -> dict[str, str]:
    return {
        "執行日": row.execute_date,
        "QQQ動能": f"{row.qqq_score:.2%}",
        "TLT動能": f"{row.tlt_score:.2%}",
        "組合": row.combo,
        "標的": row.target,
        "換倉": "YES" if row.changed else "NO",
    }


def checks_df(result: core.WorkflowResult) -> pd.DataFrame:
    return pd.DataFrame([asdict(item) for item in result.checks]).rename(
        columns={"status": "狀態", "item": "項目", "detail": "說明"}
    )


def stats_df(result: core.WorkflowResult) -> pd.DataFrame:
    rows = []
    for stat in result.stats:
        rows.append(
            {
                "標的": stat.symbol,
                "來源": stat.source,
                "最早月": stat.first_month,
                "最新月": stat.latest_month,
                "最新月K": "" if stat.latest_price is None else f"{stat.latest_price:.4f}",
                "筆數": stat.rows,
                "比對結果": stat.compare_result,
                "缺月份": ", ".join(stat.missing_months) if stat.missing_months else "-",
            }
        )
    return pd.DataFrame(rows)


def price_usage_df(result: core.WorkflowResult) -> pd.DataFrame:
    rows = []
    _, months = core.latest_price_context(result.rows)
    roles = ["基準月", "1個月", "3個月", "6個月"]
    for symbol in core.SIGNAL_SYMBOLS:
        prices = result.prices.get(symbol, {})
        source = result.used_sources.get(symbol, core.SOURCE_YAHOO)
        for role, month in zip(roles, months):
            price = prices.get(month)
            rows.append(
                {
                    "標的": symbol,
                    "用途": role,
                    "月份": month,
                    "Adj Close": "" if price is None else f"{price:.4f}",
                    "來源": source,
                    "Alpha比對": core.build_price_compare_text(symbol, month, prices, result.alpha_prices),
                }
            )
    return pd.DataFrame(rows)


def history_df(rows: list[core.SignalRow]) -> pd.DataFrame:
    return pd.DataFrame([signal_to_dict(row) for row in rows[-24:]])


def card_class(status: str) -> str:
    if status == "OK":
        return "ok-card"
    if status == "FAIL":
        return "fail-card"
    return "skip-card"


def render_signal(result: core.WorkflowResult) -> None:
    latest = result.rows[-1]
    _, months = core.latest_price_context(result.rows)
    date_text = f"月底預估 {latest.execute_date}" if result.is_preview else latest.execute_date
    change_text = "需要換倉" if latest.changed else "不用換倉"
    change_class = "change-yes" if latest.changed else "change-no"
    score_prefix = "預估計算結果" if result.is_preview else "計算結果"
    month_label = "預估基準月" if result.is_preview else "計算月份"
    st.markdown(
        f"""
        <div class="signal-box">
            <div class="signal-date">{date_text}</div>
            <div>
                <span style="font-size:1.25rem;font-weight:800;">建議標的：</span>
                <span class="signal-target">{latest.target}</span>
                <span class="signal-change {change_class}">{change_text}</span>
            </div>
            <div class="score-line">{score_prefix}｜QQQ {latest.qqq_score:.2%} / TLT {latest.tlt_score:.2%}</div>
            <div class="muted-line">來源：{result.source_summary}</div>
            <div class="muted-line">{month_label}：{months[0]} 對比 {months[1]} / {months[2]} / {months[3]}</div>
            <div class="muted-line">{result.log_path}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def style_checks(df: pd.DataFrame):
    def row_style(row: pd.Series) -> list[str]:
        if row["狀態"] == "OK":
            color = "background-color: #dcfce7"
        elif row["狀態"] == "FAIL":
            color = "background-color: #fecaca"
        else:
            color = "background-color: #fde68a"
        return [color] * len(row)

    display = df.copy()
    display["狀態"] = display["狀態"].map({"OK": "✓", "FAIL": "✕", "SKIP": "-"}).fillna(display["狀態"])
    return display.style.apply(row_style, axis=1)


def style_price_usage(df: pd.DataFrame):
    def row_style(row: pd.Series) -> list[str]:
        color = ""
        if row["用途"] == "基準月":
            color = "background-color: #fef3c7"
        if str(row["Alpha比對"]).startswith("差異") or "缺" in str(row["Alpha比對"]):
            color = "background-color: #fecaca"
        return [color] * len(row)

    return df.style.apply(row_style, axis=1)


def style_history(df: pd.DataFrame):
    def row_style(row: pd.Series) -> list[str]:
        color = "background-color: #ffe4e6" if row["換倉"] == "YES" else ""
        return [color] * len(row)

    return df.style.apply(row_style, axis=1)


def run(mode: str, alpha_key: str) -> None:
    preview = mode == "preview"
    with st.spinner("抓價與計算中..."):
        st.session_state.result = core.run_workflow(
            core.SIGNAL_START_MONTH,
            preview,
            core.SOURCE_YAHOO_ALPHA_BACKUP,
            alpha_key,
        )
        st.session_state.last_mode = mode


def load_existing_result() -> core.WorkflowResult | None:
    rows = core.keep_formal_rows(core.read_signal_log())
    if not rows:
        return None
    cutoff = core.latest_completed_month()
    prices = {
        symbol: core.filter_prices_through(core.read_cached_prices(symbol, core.SOURCE_YAHOO), cutoff)
        for symbol in core.SIGNAL_SYMBOLS
    }
    used_sources = {symbol: core.SOURCE_YAHOO for symbol in core.SIGNAL_SYMBOLS}
    stats = [
        core.build_price_stats(symbol, core.SOURCE_YAHOO, prices[symbol], "")
        for symbol in core.SIGNAL_SYMBOLS
    ]
    checks = [core.CheckItem("SKIP", "啟動載入", "已載入既有正式紀錄，按正式計算或月底預估可更新。")]
    return core.WorkflowResult(rows, str(core.SIGNAL_LOG), "QQQ:yahoo/TLT:yahoo", used_sources, [], stats, prices, {}, checks)


def main() -> None:
    if not require_password():
        return

    if "page" not in st.session_state:
        st.session_state.page = "首頁"

    st.markdown('<div class="topbar"><div class="top-title">2倍大盤動能計算</div></div>', unsafe_allow_html=True)

    nav_cols = st.columns([4.8, 1, 1, .75, .9], gap="small")
    with nav_cols[1]:
        formal_clicked = st.button("正式計算", use_container_width=True)
    with nav_cols[2]:
        preview_clicked = st.button("月底預估", use_container_width=True)
    with nav_cols[3]:
        if st.button("首頁", use_container_width=True):
            st.session_state.page = "首頁"
    with nav_cols[4]:
        if st.button("使用說明", use_container_width=True):
            st.session_state.page = "使用說明"

    option_cols = st.columns([1.1, 1.6, 4.5], gap="small")
    with option_cols[0]:
        st.markdown('<div class="option-row"><b>資料來源：Yahoo</b></div>', unsafe_allow_html=True)
    with option_cols[1]:
        st.text_input("Alpha Key（備援/後台比對）", type="password", key="alpha_key", label_visibility="collapsed")
    with option_cols[2]:
        st.markdown('<div class="option-row"><b style="color:#2563eb;">正式計算只用完成月 K；月底預估使用本月目前價格</b></div>', unsafe_allow_html=True)

    if formal_clicked or preview_clicked:
        st.session_state.page = "首頁"
        try:
            run("preview" if preview_clicked else "formal", st.session_state.get("alpha_key", ""))
            st.session_state.pop("run_error", None)
        except Exception as exc:
            st.session_state.run_error = str(exc)

    if "result" not in st.session_state:
        existing = load_existing_result()
        if existing:
            st.session_state.result = existing

    if st.session_state.page == "使用說明":
        st.markdown('<div class="section-title">使用說明</div>', unsafe_allow_html=True)
        st.markdown(
            """
            <div class="help-box">

            **正式計算**：只使用已完成月 K，結果會寫入正式紀錄。

            **月底預估**：使用本月目前價格暫代月底月 K，提前預估下一個執行月，不寫入正式紀錄。

            **Alpha Key**：選填。未輸入時只用 Yahoo；有輸入時會做 Alpha Vantage 後台比對。若 Alpha 線上失敗但本地快取有資料，會用快取顯示比對百分比。

            **Alpha 比對欄位**：
            - 空白：未輸入 Alpha Key，沒有比對。
            - 正常 +0.xx%：兩邊價格差異在可接受範圍。
            - 差異 +x.xx%：差異超過門檻，請人工確認 Yahoo / Alpha 價格。
            - Alpha缺資料 / Yahoo缺資料：該月份缺資料，正式採用前請人工核對。

            **快取保護**：5 分鐘內重複按相同計算，會優先使用本地快取，避免一直線上抓價。

            **手機使用**：本機測試時，手機需和電腦在同一個 Wi-Fi，再用電腦區網 IP 加上 `:8501` 開啟。

            </div>
            """
            ,
            unsafe_allow_html=True,
        )
        return

    result = st.session_state.get("result")
    if not result:
        st.info("尚無紀錄。請先按「正式計算」或「月底預估」。")
        if st.session_state.get("run_error"):
            st.error(st.session_state.run_error)
        return

    if st.session_state.get("run_error"):
        st.warning(f"本次線上取價未完成，畫面保留既有資料：{st.session_state.run_error}")

    top_left, top_right = st.columns([1.08, 1], gap="small")
    with top_left:
        st.markdown('<div class="section-title">最新訊號</div>', unsafe_allow_html=True)
        render_signal(result)
    with top_right:
        st.markdown('<div class="section-title">檢查項目</div>', unsafe_allow_html=True)
        st.dataframe(style_checks(checks_df(result)), hide_index=True, use_container_width=True, height=205)

    bottom_left, bottom_right = st.columns([.92, 1.08], gap="small")
    with bottom_left:
        st.markdown('<div class="section-title">資料檢查</div>', unsafe_allow_html=True)
        st.dataframe(stats_df(result), hide_index=True, use_container_width=True, height=115)
        st.markdown('<div class="section-title">本次計算用價</div>', unsafe_allow_html=True)
        st.dataframe(style_price_usage(price_usage_df(result)), hide_index=True, use_container_width=True, height=285)
    with bottom_right:
        st.markdown('<div class="section-title">動能歷史紀錄</div>', unsafe_allow_html=True)
        st.dataframe(style_history(history_df(result.rows)), hide_index=True, use_container_width=True, height=435)


if __name__ == "__main__":
    main()
