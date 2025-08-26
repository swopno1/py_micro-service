import yfinance as yf
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

def get_ticker_info(symbol: str) -> Optional[Dict[str, Any]]:
    """A helper function to fetch and cache yfinance ticker info."""
    try:
        stock = yf.Ticker(symbol)
        # The .info dictionary is fetched from Yahoo Finance
        info = stock.info
        if not info or info.get('trailingPegRatio') is None: # check for empty or incomplete info
            logger.warning(f"Incomplete or empty data for {symbol} from yfinance. Ticker might be delisted or invalid.")
            return None
        return info
    except Exception as e:
        logger.error(f"Error creating yf.Ticker for {symbol}: {e}", exc_info=True)
        return None

def fetch_symbol_meta(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Fetches fundamental and profile data for a given symbol.
    """
    info = get_ticker_info(symbol)
    if not info:
        return None

    return {
        "symbol": symbol.upper(),
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "marketCap": info.get("marketCap"),
        "dividendYield": info.get("dividendYield"),
        "debtEq": info.get("debtToEquity"),
        "rOE": info.get("returnOnEquity"),
        "website": info.get("website"),
        "country": info.get("country"),
        "description": info.get("longBusinessSummary"),
        "logo": info.get("logo_url"),
        "source": "YAHOO",
        "lastUpdated": datetime.utcnow(),
    }

def fetch_symbol_quote(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Fetches the latest quote data (price, volume, etc.) for a given symbol.
    """
    info = get_ticker_info(symbol)
    if not info:
        return None

    return {
        "symbol": symbol.upper(),
        "price": info.get("regularMarketPrice"),
        "change": info.get("regularMarketChange"),
        "volume": info.get("regularMarketVolume"),
        "pE": info.get("trailingPE"),
        "pEG": info.get("trailingPegRatio"),
        "pB": info.get("priceToBook"),
        "beta": info.get("beta"),
        "w52High": info.get("fiftyTwoWeekHigh"),
        "w52Low": info.get("fiftyTwoWeekLow"),
        "recommendation": info.get("recommendationKey"),
        "lastUpdated": datetime.utcnow(),
    }

def fetch_stock_price_history(symbol: str, period: str = "1d") -> List[Dict[str, Any]]:
    """
    Fetches the daily OHLCV history for a given symbol.
    Returns a list of records, one for each day.
    """
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period=period)

        if hist.empty:
            logger.warning(f"No history data for symbol {symbol} for the given period.")
            return []

        # Convert dataframe to list of dictionaries
        hist = hist.reset_index()
        records = []
        for _, row in hist.iterrows():
            records.append({
                "symbol": symbol.upper(),
                "date": row["Date"].date(),
                "open": row["Open"],
                "high": row["High"],
                "low": row["Low"],
                "close": row["Close"],
                "volume": int(row["Volume"]),
            })
        return records
    except Exception as e:
        logger.error(f"Error fetching price history for {symbol}: {e}", exc_info=True)
        return []