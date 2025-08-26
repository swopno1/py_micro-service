import yfinance as yf
import logging
import time
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger(__name__)

# Constants for retry logic
MAX_RETRIES = 3
BACKOFF_FACTOR = 2

def _perform_with_retries(api_call, symbol: str) -> Tuple[str, Any]:
    """A wrapper to perform an API call with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            result = api_call()
            if result is None or (hasattr(result, 'empty') and result.empty):
                 # Handles cases where yfinance returns None or empty DataFrame
                logger.warning(f"No data returned for {symbol} on attempt {attempt + 1}.")
                # We don't retry on empty data, we consider it a final state.
                return 'delisted', None
            return 'ok', result
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = BACKOFF_FACTOR * (2 ** attempt)
                logger.warning(f"Rate limited on {symbol}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            elif e.response.status_code == 404:
                logger.warning(f"404 Not Found for {symbol}. Marking as delisted.")
                return 'delisted', None
            else:
                logger.error(f"HTTP Error for {symbol}: {e}", exc_info=True)
                return 'fetch_failed', None
        except Exception as e:
            logger.error(f"Unexpected error for {symbol}: {e}", exc_info=True)
            return 'fetch_failed', None
    logger.error(f"API call for {symbol} failed after all retries.")
    return 'fetch_failed', None

def get_ticker_info(symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Fetches yfinance ticker info with retries and status."""
    stock = yf.Ticker(symbol)

    status, info = _perform_with_retries(lambda: stock.info, symbol)
    if status != 'ok':
        return status, None

    if not info or info.get('trailingPegRatio') is None:
        logger.warning(f"Incomplete data for {symbol}. Marking as delisted.")
        return 'delisted', None

    return 'ok', info

def fetch_symbol_meta(symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Fetches fundamental data for a given symbol."""
    status, info = get_ticker_info(symbol)
    if status != 'ok':
        return status, None

    data = {
        "symbol": symbol.upper(), "sector": info.get("sector"),
        "industry": info.get("industry"), "marketCap": info.get("marketCap"),
        "dividendYield": info.get("dividendYield"), "debtEq": info.get("debtToEquity"),
        "rOE": info.get("returnOnEquity"), "website": info.get("website"),
        "country": info.get("country"), "description": info.get("longBusinessSummary"),
        "logo": info.get("logo_url"), "source": "YAHOO",
        "lastUpdated": datetime.utcnow(),
    }
    return 'ok', data

def fetch_symbol_quote(symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Fetches the latest quote data for a given symbol."""
    status, info = get_ticker_info(symbol)
    if status != 'ok':
        return status, None

    data = {
        "symbol": symbol.upper(), "price": info.get("regularMarketPrice"),
        "change": info.get("regularMarketChange"), "volume": info.get("regularMarketVolume"),
        "pE": info.get("trailingPE"), "pEG": info.get("trailingPegRatio"),
        "pB": info.get("priceToBook"), "beta": info.get("beta"),
        "w52High": info.get("fiftyTwoWeekHigh"), "w52Low": info.get("fiftyTwoWeekLow"),
        "recommendation": info.get("recommendationKey"), "lastUpdated": datetime.utcnow(),
    }
    return 'ok', data

def fetch_stock_price_history(symbol: str, period: str = "1d") -> Tuple[str, List[Dict[str, Any]]]:
    """Fetches the daily OHLCV history for a given symbol with retries."""
    stock = yf.Ticker(symbol)

    status, hist = _perform_with_retries(lambda: stock.history(period=period), symbol)
    if status != 'ok':
        return status, []

    hist = hist.reset_index()
    records = [
        {
            "symbol": symbol.upper(), "date": row["Date"].date(),
            "open": row["Open"], "high": row["High"],
            "low": row["Low"], "close": row["Close"],
            "volume": int(row["Volume"]),
        }
        for _, row in hist.iterrows()
    ]
    return 'ok', records