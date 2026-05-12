# 2倍大盤動能計算 Streamlit 版

這是 Streamlit Community Cloud 部署用的版本。

## 功能

- 正式計算：使用已完成月 K，結果寫入正式紀錄。
- 月底預估：使用本月目前價格暫代月底月 K，提前預估下一個執行月，不寫入正式紀錄。
- Yahoo 為主要資料來源，不需要 API Key。
- Alpha Vantage Key 選填，用於後台比對與備援。
- 5 分鐘快取保護，避免連續重複抓價。

## Streamlit Cloud 設定

Main file path:

```text
streamlit_app.py
```

Python version 建議使用：

```text
3.12
```

Secrets 可設定密碼：

```toml
APP_PASSWORD = "your-password"
```

未設定 `APP_PASSWORD` 時，任何取得網址的人都可以開啟。

## 必要檔案

```text
streamlit_app.py
two_x_market_app.py
requirements.txt
.streamlit/config.toml
two_x_market_records/
```

## 注意

本工具是投資輔助計算工具，不是投資建議。正式紀錄以「正式計算」結果為準；月底預估只供提前觀察。
