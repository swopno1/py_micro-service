import yfinance as yf
import logging
import time
import requests
import finnhub
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# --- Helper Functions ---

def _perform_with_retries(api_call, symbol: str) -> Tuple[str, Any]:
    """A wrapper to perform an API call with exponential backoff."""
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 2
    for attempt in range(MAX_RETRIES):
        try:
            result = api_call()
            if result is None or (hasattr(result, 'empty') and result.empty):
                logger.warning(f"No data returned for {symbol} on attempt {attempt + 1}.")
                return 'no_data', None
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

# --- Data Source Abstraction ---

class DataSource(ABC):
    """Abstract base class for data sources."""
    @abstractmethod
    def fetch_symbol_meta(self, symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        raise NotImplementedError

    @abstractmethod
    def fetch_symbol_quote(self, symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        raise NotImplementedError

    @abstractmethod
    def fetch_stock_price_history(self, symbol: str, period: str = "1d") -> Tuple[str, List[Dict[str, Any]]]:
        raise NotImplementedError

# --- Yahoo Finance Implementation ---

class YahooFinanceSource(DataSource):
    """Data source for Yahoo Finance."""
    def _get_ticker_info(self, symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Fetches yfinance ticker info with retries and status."""
        stock = yf.Ticker(symbol)
        status, info = _perform_with_retries(lambda: stock.info, symbol)
        if status != 'ok':
            return status, None
        if not info or info.get('trailingPegRatio') is None:
            logger.warning(f"Incomplete data for {symbol} from Yahoo Finance.")
            return 'no_data', None
        return 'ok', info

    def fetch_symbol_meta(self, symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Fetches fundamental data for a given symbol from Yahoo Finance."""
        status, info = self._get_ticker_info(symbol)
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

    def fetch_symbol_quote(self, symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Fetches the latest quote data for a given symbol from Yahoo Finance."""
        status, info = self._get_ticker_info(symbol)
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

    def fetch_stock_price_history(self, symbol: str, period: str = "1d") -> Tuple[str, List[Dict[str, Any]]]:
        """Fetches the daily OHLCV history for a given symbol from Yahoo Finance."""
        stock = yf.Ticker(symbol)
        status, hist = _perform_with_retries(lambda: stock.history(period=period), symbol)
        if status != 'ok':
            return status, []
        hist = hist.reset_index()
        records = [
            {
                "symbol": symbol.upper(), "date": row["Date"].to_pydatetime().date(),
                "open": row["Open"], "high": row["High"],
                "low": row["Low"], "close": row["Close"],
                "volume": int(row["Volume"]),
            }
            for _, row in hist.iterrows()
        ]
        return 'ok', records

# --- Finnhub Implementation ---

class FinnhubSource(DataSource):
    """Data source for Finnhub."""
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("FINNHUB_API_KEY")
        if not self.api_key:
            logger.warning("Finnhub API key not found. FinnhubSource will be disabled.")
            self.client = None
        else:
            self.client = finnhub.Client(api_key=self.api_key)

    def fetch_symbol_meta(self, symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        if not self.client:
            return 'fetch_failed', None

        status, profile = _perform_with_retries(lambda: self.client.company_profile2(symbol=symbol), symbol)
        if status != 'ok' or not profile:
            return status, None

        data = {
            "symbol": profile.get("ticker", symbol).upper(),
            "sector": profile.get("finnhubIndustry"),
            "industry": None, # Finnhub does not provide a direct equivalent
            "marketCap": profile.get("marketCapitalization"),
            "dividendYield": profile.get("dividendYield"),
            "debtEq": None, # Finnhub does not provide a direct equivalent
            "rOE": None, # Finnhub does not provide a direct equivalent
            "website": profile.get("weburl"),
            "country": profile.get("country"),
            "description": None, # Finnhub does not provide a direct equivalent
            "logo": profile.get("logo"),
            "source": "FINNHUB",
            "lastUpdated": datetime.utcnow(),
        }
        return 'ok', data

    def fetch_symbol_quote(self, symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        if not self.client:
            return 'fetch_failed', None

        status, quote = _perform_with_retries(lambda: self.client.quote(symbol=symbol), symbol)
        if status != 'ok' or not quote or quote.get('c') == 0:
            return 'no_data', None

        data = {
            "symbol": symbol.upper(),
            "price": quote.get("c"),
            "change": quote.get("d"),
            "volume": None, # Finnhub quote endpoint doesn't provide volume
            "pE": None,
            "pEG": None,
            "pB": None,
            "beta": None,
            "w52High": None,
            "w52Low": None,
            "recommendation": None,
            "lastUpdated": datetime.fromtimestamp(quote.get("t")) if quote.get("t") else datetime.utcnow(),
        }
        return 'ok', data

    def fetch_stock_price_history(self, symbol: str, period: str = "1d") -> Tuple[str, List[Dict[str, Any]]]:
        if not self.client:
            return 'fetch_failed', None

        # Finnhub uses start and end timestamps, not a period string like "1d"
        # We'll fetch the last day of data.
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=1)

        status, hist = _perform_with_retries(
            lambda: self.client.stock_candles(
                symbol, 'D', int(start_date.timestamp()), int(end_date.timestamp())
            ),
            symbol
        )

        if status != 'ok' or not hist or hist.get('s') != 'ok':
            return status if status != 'ok' else 'no_data', []

        records = []
        for i in range(len(hist['t'])):
            records.append({
                "symbol": symbol.upper(),
                "date": datetime.fromtimestamp(hist['t'][i]).date(),
                "open": hist['o'][i],
                "high": hist['h'][i],
                "low": hist['l'][i],
                "close": hist['c'][i],
                "volume": int(hist['v'][i]),
            })

        return 'ok', records

# --- Fetcher Manager ---

class FetcherManager:
    """Manages multiple data sources and rotates through them."""
    def __init__(self):
        self.sources = []
        self.current_source_index = 0

        # Initialize and add Yahoo Finance source
        self.sources.append(YahooFinanceSource())

        # Initialize and add Finnhub source if API key is available
        finnhub_source = FinnhubSource()
        if finnhub_source.client:
            self.sources.append(finnhub_source)

        logger.info(f"FetcherManager initialized with {len(self.sources)} data source(s).")

    def _get_next_source(self) -> DataSource:
        """Returns the next data source in a round-robin fashion."""
        if not self.sources:
            raise Exception("No data sources available.")
        source = self.sources[self.current_source_index]
        self.current_source_index = (self.current_source_index + 1) % len(self.sources)
        logger.info(f"Using {source.__class__.__name__} for the next fetch.")
        return source

    def fetch_symbol_meta(self, symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        source = self._get_next_source()
        return source.fetch_symbol_meta(symbol)

    def fetch_symbol_quote(self, symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        source = self._get_next_source()
        return source.fetch_symbol_quote(symbol)

    def fetch_stock_price_history(self, symbol: str, period: str = "1d") -> Tuple[str, List[Dict[str, Any]]]:
        source = self._get_next_source()
        return source.fetch_stock_price_history(symbol, period)

# --- Singleton instance of the manager ---
fetcher_manager = FetcherManager()

# --- Global access functions ---

def fetch_symbol_meta(symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Fetches fundamental data for a given symbol using the fetcher manager."""
    return fetcher_manager.fetch_symbol_meta(symbol)

def fetch_symbol_quote(symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Fetches the latest quote data for a given symbol using the fetcher manager."""
    return fetcher_manager.fetch_symbol_quote(symbol)

def fetch_stock_price_history(symbol: str, period: str = "1d") -> Tuple[str, List[Dict[str, Any]]]:
    """Fetches the daily OHLCV history for a given symbol using the fetcher manager."""
    return fetcher_manager.fetch_stock_price_history(symbol, period)