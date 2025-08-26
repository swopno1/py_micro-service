import os
import sys
import requests
from bs4 import BeautifulSoup
import logging
from cuid2 import Cuid

# Add the parent directory to the sys.path to allow imports from the 'api' module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.database import get_db_connection

# --- Logging Setup ---
log_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger = logging.getLogger(__name__)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

# CUID generator
cuid_generator = Cuid()


def get_sp500_symbols():
    """
    Fetches S&P 500 symbols from a reliable source.
    """
    # Using a known reliable source for S&P 500 components
    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes

        # The first line is the header, so we skip it
        lines = response.text.strip().split('\n')[1:]
        symbols = []
        for line in lines:
            parts = line.split(',')
            if len(parts) >= 2:
                # Symbol is the first part, Name is the second
                symbol = parts[0].strip()
                name = parts[1].strip()
                symbols.append({"symbol": symbol, "name": name})
        return symbols
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching S&P 500 symbols: {e}")
        return []


def populate_symbols_master(symbols):
    """
    Populates the SymbolsMaster table with the given symbols.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            for stock in symbols:
                try:
                    insert_query = """
                        INSERT INTO "SymbolsMaster" (id, symbol, name, source, status, "lastUpdated")
                        VALUES (%s, %s, %s, 'YAHOO', 'active', NOW())
                        ON CONFLICT (symbol) DO NOTHING;
                    """
                    cursor.execute(insert_query, (cuid_generator.generate(), stock['symbol'], stock['name']))
                    logger.info(f"Inserted/updated symbol: {stock['symbol']}")
                except Exception as e:
                    logger.error(f"Error inserting symbol {stock['symbol']}: {e}")
                    conn.rollback() # Rollback on error for a specific symbol
            conn.commit()
            logger.info("Successfully populated SymbolsMaster table.")
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    logger.info("Starting symbol population script.")

    # S&P 500
    sp500_symbols = get_sp500_symbols()
    if sp500_symbols:
        logger.info(f"Found {len(sp500_symbols)} S&P 500 symbols.")
        populate_symbols_master(sp500_symbols)
    else:
        logger.warning("Could not retrieve S&P 500 symbols.")

    # International stocks
    international_symbols = [
        # India
        {"symbol": "RELIANCE.NS", "name": "Reliance Industries"},
        {"symbol": "TCS.NS", "name": "Tata Consultancy Services"},
        {"symbol": "HDFCBANK.NS", "name": "HDFC Bank"},
        {"symbol": "INFY.NS", "name": "Infosys"},
        {"symbol": "HINDUNILVR.NS", "name": "Hindustan Unilever"},
        # Japan
        {"symbol": "7203.T", "name": "Toyota Motor"},
        {"symbol": "9984.T", "name": "Softbank Group"},
        {"symbol": "6758.T", "name": "Sony"},
        {"symbol": "9432.T", "name": "Nippon Telegraph & Telephone"},
        {"symbol": "8306.T", "name": "Mitsubishi UFJ Financial Group"},
        # China
        {"symbol": "0700.HK", "name": "Tencent Holdings"},
        {"symbol": "9988.HK", "name": "Alibaba Group"},
        {"symbol": "3690.HK", "name": "Meituan"},
        {"symbol": "1211.HK", "name": "BYD Company"},
        {"symbol": "2382.HK", "name": "Sunny Optical Technology"},
    ]
    logger.info(f"Adding {len(international_symbols)} international symbols.")
    populate_symbols_master(international_symbols)

    logger.info("Script finished.")
