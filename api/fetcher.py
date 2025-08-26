import yfinance as yf
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def fetch_stock_data(symbol: str):
    try:
        stock = yf.Ticker(symbol)
        info = stock.info
        hist = stock.history(period="1d")

        if hist.empty:
            logger.warning(f"No history data for symbol {symbol}")
            return None

        latest_hist = hist.iloc[-1]

        # Prepare data for all tables
        symbol_master_data = {
            "symbol": symbol,
            "name": info.get("longName"),
            "exchange": info.get("exchange"),
            "type": info.get("quoteType"),
            "currency": info.get("currency"),
            "status": "active",  # Assuming active if data is fetched
            "source": "YAHOO",
            "lastUpdated": datetime.utcnow(),
        }

        symbol_meta_data = {
            "symbol": symbol,
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

        symbol_quote_data = {
            "symbol": symbol,
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

        stock_price_data = {
            "symbol": symbol,
            "date": hist.index[-1].date(),
            "open": latest_hist["Open"],
            "high": latest_hist["High"],
            "low": latest_hist["Low"],
            "close": latest_hist["Close"],
            "volume": int(latest_hist["Volume"]),
        }

        return {
            "master": symbol_master_data,
            "meta": symbol_meta_data,
            "quote": symbol_quote_data,
            "price": stock_price_data,
        }

    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}", exc_info=True)
        return None


def fetch_eod_data(symbol: str):
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="1d")

        if hist.empty:
            logger.warning(f"No history data for symbol {symbol}")
            return None

        latest = hist.iloc[-1]
        return {
            "symbol": symbol,
            "date": hist.index[-1].date(),  # Get date from the data index
            "open": latest["Open"],
            "high": latest["High"],
            "low": latest["Low"],
            "close": latest["Close"],
            "volume": int(latest["Volume"]),  # Ensure volume is an integer
        }
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}", exc_info=True)
        return None