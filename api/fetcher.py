import yfinance as yf
import logging
import time
import requests
import finnhub
import os
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData
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
        except finnhub.exceptions.FinnhubAPIException as e:
            if e.status_code == 429:
                wait_time = BACKOFF_FACTOR * (2 ** attempt)
                logger.warning(f"Rate limited on {symbol} by Finnhub. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"Finnhub API Error for {symbol}: {e}", exc_info=True)
                return 'fetch_failed', None
        except ValueError as e:
            if "call frequency" in str(e):
                wait_time = BACKOFF_FACTOR * (2 ** attempt)
                logger.warning(f"Rate limited on {symbol} by Alpha Vantage. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"Alpha Vantage Value Error for {symbol}: {e}", exc_info=True)
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
        if status != 'ok':
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
        if status != 'ok' or quote.get('c') == 0:
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

        if status != 'ok' or hist.get('s') != 'ok':
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

# --- Alpha Vantage Implementation ---

class AlphaVantageSource(DataSource):
    """Data source for Alpha Vantage."""
    def __init__(self, api_keys: Optional[List[str]] = None):
        if api_keys:
            self.api_keys = api_keys
        else:
            self.api_keys = [val for key, val in os.environ.items() if key.startswith("ALPHAVANTAGE") and val]

        if not self.api_keys:
            logger.warning("Alpha Vantage API keys not found. AlphaVantageSource will be disabled.")
            self.clients = []
        else:
            self.clients = [
                (TimeSeries(key=key, output_format='pandas'), FundamentalData(key=key, output_format='pandas'))
                for key in self.api_keys
            ]
        self.current_client_index = 0

    def _get_next_client(self):
        """Rotates through the available Alpha Vantage clients."""
        if not self.clients:
            return None, None
        client_pair = self.clients[self.current_client_index]
        self.current_client_index = (self.current_client_index + 1) % len(self.clients)
        return client_pair

    def fetch_symbol_meta(self, symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        ts_client, fd_client = self._get_next_client()
        if not fd_client:
            return 'fetch_failed', None

        try:
            status, overview = _perform_with_retries(lambda: fd_client.get_company_overview(symbol), symbol)
            if status != 'ok' or overview.empty:
                return 'no_data', None

            profile = overview.iloc[0].to_dict()
            data = {
                "symbol": profile.get("Symbol", symbol).upper(),
                "sector": profile.get("Sector"),
                "industry": profile.get("Industry"),
                "marketCap": float(profile.get("MarketCapitalization", 0)),
                "dividendYield": float(profile.get("DividendYield", 0)),
                "debtEq": float(profile.get("DebtToEquityRatio", 0)),
                "rOE": float(profile.get("ReturnOnEquityTTM", 0)),
                "website": None,
                "country": profile.get("Country"),
                "description": profile.get("Description"),
                "logo": None,
                "source": "ALPHAVANTAGE",
                "lastUpdated": datetime.utcnow(),
            }
            return 'ok', data
        except Exception as e:
            logger.error(f"Alpha Vantage meta fetch failed for {symbol}: {e}", exc_info=True)
            return 'fetch_failed', None

    def fetch_symbol_quote(self, symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        ts_client, _ = self._get_next_client()
        if not ts_client:
            return 'fetch_failed', None

        try:
            status, quote_data = _perform_with_retries(lambda: ts_client.get_quote_endpoint(symbol), symbol)
            if status != 'ok' or quote_data.empty:
                return 'no_data', None

            quote = quote_data.iloc[0].to_dict()
            data = {
                "symbol": symbol.upper(),
                "price": float(quote.get("05. price", 0)),
                "change": float(quote.get("09. change", 0)),
                "volume": int(quote.get("06. volume", 0)),
                "pE": None, "pEG": None, "pB": None, "beta": None,
                "w52High": None, "w52Low": None, "recommendation": None,
                "lastUpdated": datetime.strptime(quote.get("07. latest trading day"), '%Y-%m-%d') if quote.get("07. latest trading day") else datetime.utcnow(),
            }
            return 'ok', data
        except Exception as e:
            logger.error(f"Alpha Vantage quote fetch failed for {symbol}: {e}", exc_info=True)
            return 'fetch_failed', None

    def fetch_stock_price_history(self, symbol: str, period: str = "1d") -> Tuple[str, List[Dict[str, Any]]]:
        ts_client, _ = self._get_next_client()
        if not ts_client:
            return 'fetch_failed', None

        try:
            status, (data, _) = _perform_with_retries(lambda: ts_client.get_daily(symbol=symbol, outputsize='compact'), symbol)
            if status != 'ok' or data.empty:
                return 'no_data', []

            data = data.reset_index()
            records = [
                {
                    "symbol": symbol.upper(),
                    "date": row["date"].date(),
                    "open": row["1. open"],
                    "high": row["2. high"],
                    "low": row["3. low"],
                    "close": row["4. close"],
                    "volume": int(row["5. volume"]),
                }
                for _, row in data.iterrows()
            ]
            return 'ok', records
        except Exception as e:
            logger.error(f"Alpha Vantage price history fetch failed for {symbol}: {e}", exc_info=True)
            return 'fetch_failed', None


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

        # Initialize and add Alpha Vantage source if API keys are available
        alpha_vantage_source = AlphaVantageSource()
        if alpha_vantage_source.clients:
            self.sources.append(alpha_vantage_source)

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

def fetch_symbols_meta_batch(symbols: List[str]) -> List[Tuple[str, Optional[Dict[str, Any]]]]:
    """Fetches fundamental metadata for a batch of symbols."""
    results = []
    try:
        tickers = yf.Tickers(' '.join(symbols))
        for symbol in symbols:
            try:
                info = tickers.tickers[symbol.upper()].info
                if not info or info.get('trailingPegRatio') is None:
                    logger.warning(f"Incomplete data for {symbol}.")
                    results.append(('no_data', None))
                    continue

                data = {
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
                results.append(('ok', data))

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.warning(f"404 Not Found for {symbol}. Marking as delisted.")
                    results.append(('delisted', None))
                else:
                    logger.error(f"HTTP Error for {symbol}: {e}", exc_info=True)
                    results.append(('fetch_failed', None))
            except Exception:
                # This catches other errors like missing 'info'
                logger.warning(f"Could not retrieve info for {symbol}. It might be delisted or invalid.")
                results.append(('delisted', None))

    except Exception as e:
        logger.error(f"Batch fetch failed for symbols: {symbols}. Error: {e}", exc_info=True)
        # If the whole batch fails, mark all as failed
        return [('fetch_failed', None)] * len(symbols)

    return results

def fetch_symbol_quote(symbol: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Fetches the latest quote data for a given symbol using the fetcher manager."""
    return fetcher_manager.fetch_symbol_quote(symbol)

def fetch_stock_price_history(symbol: str, period: str = "1d") -> Tuple[str, List[Dict[str, Any]]]:
    """Fetches the daily OHLCV history for a given symbol using the fetcher manager."""
    return fetcher_manager.fetch_stock_price_history(symbol, period)