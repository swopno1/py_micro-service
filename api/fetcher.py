import yfinance as yf
import logging
import time
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

# Constants for retry logic
MAX_RETRIES = 5
BACKOFF_FACTOR = 2  # Seconds

def _perform_with_retries(api_call, symbol: str):
    """A wrapper to perform an API call with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            return api_call()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = BACKOFF_FACTOR * (2 ** attempt)
                logger.warning(
                    f"Rate limited on symbol {symbol}. Retrying in {wait_time} seconds... (Attempt {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(wait_time)
            else:
                logger.error(f"HTTP Error for symbol {symbol}: {e}", exc_info=True)
                return None
        except Exception as e:
            # Handles other potential errors like json.JSONDecodeError
            logger.error(f"An unexpected error occurred for symbol {symbol}: {e}", exc_info=True)
            return None
    logger.error(f"API call for symbol {symbol} failed after all retries.")
    return None

def get_ticker_info(symbol: str) -> Optional[Dict[str, Any]]:
    """A helper function to fetch yfinance ticker info with retries."""
    stock = yf.Ticker(symbol)

    info = _perform_with_retries(lambda: stock.info, symbol)

    if not info or info.get('trailingPegRatio') is None:
        # This check helps filter out invalid symbols or empty responses
        logger.warning(f"Incomplete or empty data for {symbol} from yfinance.")
        return None
    return info

def fetch_symbol_meta(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetches fundamental and profile data for a given symbol."""
    info = get_ticker_info(symbol)
    if not info:
        return None

    return {
        "symbol": symbol.upper(), "sector": info.get("sector"),
        "industry": info.get("industry"), "marketCap": info.get("marketCap"),
        "dividendYield": info.get("dividendYield"), "debtEq": info.get("debtToEquity"),
        "rOE": info.get("returnOnEquity"), "website": info.get("website"),
        "country": info.get("country"), "description": info.get("longBusinessSummary"),
        "logo": info.get("logo_url"), "source": "YAHOO",
        "lastUpdated": datetime.utcnow(),
    }

def fetch_symbol_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetches the latest quote data for a given symbol."""
    info = get_ticker_info(symbol)
    if not info:
        return None

    return {
        "symbol": symbol.upper(), "price": info.get("regularMarketPrice"),
        "change": info.get("regularMarketChange"), "volume": info.get("regularMarketVolume"),
        "pE": info.get("trailingPE"), "pEG": info.get("trailingPegRatio"),
        "pB": info.get("priceToBook"), "beta": info.get("beta"),
        "w52High": info.get("fiftyTwoWeekHigh"), "w52Low": info.get("fiftyTwoWeekLow"),
        "recommendation": info.get("recommendationKey"), "lastUpdated": datetime.utcnow(),
    }

def fetch_stock_price_history(symbol: str, period: str = "1d") -> List[Dict[str, Any]]:
    """Fetches the daily OHLCV history for a given symbol with retries."""
    stock = yf.Ticker(symbol)

    hist = _perform_with_retries(lambda: stock.history(period=period), symbol)

    if hist is None or hist.empty:
        logger.warning(f"No history data for symbol {symbol} for the given period.")
        return []

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
    return records