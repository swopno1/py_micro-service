import yfinance as yf
import logging

logger = logging.getLogger(__name__)

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
            "volume": int(latest["Volume"]) # Ensure volume is an integer
        }
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}", exc_info=True)
        return None