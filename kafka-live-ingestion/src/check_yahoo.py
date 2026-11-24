"""Find out which candle sizes Yahoo provides for your tickers.

  python src/check_yahoo.py                 # tests COMI.CA, HRHO.CA, GC=F, EGP=X
  python src/check_yahoo.py COMI.CA ETEL.CA # tests the tickers you list

For each ticker and candle size it prints the number of rows and the newest
candle time, or the error. Use the result to set CANDLE_INTERVAL in .env.
"""
from __future__ import annotations

import sys

import yfinance as yf

INTERVALS = ("5m", "15m", "1h", "1d")


def main() -> int:
    symbols = sys.argv[1:] or ["COMI.CA", "HRHO.CA", "GC=F", "EGP=X"]
    for symbol in symbols:
        print(f"\n{symbol}")
        for interval in INTERVALS:
            try:
                df = yf.Ticker(symbol).history(period="5d", interval=interval, auto_adjust=False)
                last = df.index[-1] if len(df) else "-"
                print(f"  {interval:>4}: {len(df):>4} rows   newest: {last}")
            except Exception as exc:  # yfinance raises different errors for missing data
                print(f"  {interval:>4}: ERROR {type(exc).__name__}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
