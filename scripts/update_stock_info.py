import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import psycopg2
from cuid2 import Cuid

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.database import get_db_connection
from api.fetcher import fetch_stock_data
from api.models import SymbolsMeta, SymbolQuote

# --- Logging Setup ---
log_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger = logging.getLogger(__name__)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

# CUID generator
cuid_generator = Cuid()

def get_all_symbols() -> List[Dict[str, Any]]:
    """Fetches all symbols from the SymbolsMaster table."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute('SELECT id, symbol FROM "SymbolsMaster"')
            rows = cursor.fetchall()
            return [{"id": row[0], "symbol": row[1]} for row in rows]
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        return []
    finally:
        if conn:
            conn.close()

def upsert_symbol_meta(conn, symbol_id: str, meta: SymbolsMeta):
    """Upserts symbol metadata into the SymbolsMeta table."""
    try:
        with conn.cursor() as cursor:
            upsert_query = """
                INSERT INTO "SymbolsMeta" (id, "symbolId", exchange, "shortName", "longName", sector, industry, "marketCap", "enterpriseValue", "totalRevenue", "floatShares", "sharesOutstanding", "lastUpdated")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT ("symbolId") DO UPDATE SET
                    exchange = EXCLUDED.exchange,
                    "shortName" = EXCLUDED."shortName",
                    "longName" = EXCLUDED."longName",
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    "marketCap" = EXCLUDED."marketCap",
                    "enterpriseValue" = EXCLUDED."enterpriseValue",
                    "totalRevenue" = EXCLUDED."totalRevenue",
                    "floatShares" = EXCLUDED."floatShares",
                    "sharesOutstanding" = EXCLUDED."sharesOutstanding",
                    "lastUpdated" = NOW();
            """
            cursor.execute(upsert_query, (
                cuid_generator.generate(), symbol_id, meta.exchange, meta.shortName, meta.longName, meta.sector, meta.industry,
                meta.marketCap, meta.enterpriseValue, meta.totalRevenue, meta.floatShares, meta.sharesOutstanding
            ))
        logger.info(f"Upserted metadata for symbol ID {symbol_id}")
    except Exception as e:
        logger.error(f"Error upserting metadata for symbol ID {symbol_id}: {e}")
        raise

def upsert_symbol_quote(conn, symbol_id: str, quote: SymbolQuote):
    """Upserts symbol quote data into the SymbolQuote table."""
    try:
        with conn.cursor() as cursor:
            upsert_query = """
                INSERT INTO "SymbolQuote" (id, "symbolId", "previousClose", "open", "dayLow", "dayHigh", "regularMarketPreviousClose", "regularMarketOpen", "regularMarketDayLow", "regularMarketDayHigh", "fiftyTwoWeekLow", "fiftyTwoWeekHigh", "lastUpdated")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT ("symbolId") DO UPDATE SET
                    "previousClose" = EXCLUDED."previousClose",
                    "open" = EXCLUDED."open",
                    "dayLow" = EXCLUDED."dayLow",
                    "dayHigh" = EXCLUDED."dayHigh",
                    "regularMarketPreviousClose" = EXCLUDED."regularMarketPreviousClose",
                    "regularMarketOpen" = EXCLUDED."regularMarketOpen",
                    "regularMarketDayLow" = EXCLUDED."regularMarketDayLow",
                    "regularMarketDayHigh" = EXCLUDED."regularMarketDayHigh",
                    "fiftyTwoWeekLow" = EXCLUDED."fiftyTwoWeekLow",
                    "fiftyTwoWeekHigh" = EXCLUDED."fiftyTwoWeekHigh",
                    "lastUpdated" = NOW();
            """
            cursor.execute(upsert_query, (
                cuid_generator.generate(), symbol_id, quote.previousClose, quote.open, quote.dayLow, quote.dayHigh,
                quote.regularMarketPreviousClose, quote.regularMarketOpen, quote.regularMarketDayLow,
                quote.regularMarketDayHigh, quote.fiftyTwoWeekLow, quote.fiftyTwoWeekHigh
            ))
        logger.info(f"Upserted quote for symbol ID {symbol_id}")
    except Exception as e:
        logger.error(f"Error upserting quote for symbol ID {symbol_id}: {e}")
        raise

def process_symbol(symbol_info: Dict[str, Any]):
    """Fetches and upserts data for a single symbol."""
    symbol_id = symbol_info["id"]
    symbol_ticker = symbol_info["symbol"]
    logger.info(f"Processing symbol: {symbol_ticker} (ID: {symbol_id})")

    try:
        data = fetch_stock_data(symbol_ticker)
        if not data:
            logger.warning(f"No data fetched for {symbol_ticker}")
            return

        conn = None
        try:
            conn = get_db_connection()
            conn.autocommit = False  # Use a transaction for meta and quote upserts

            if data.get("meta"):
                upsert_symbol_meta(conn, symbol_id, data["meta"])

            if data.get("quote"):
                upsert_symbol_quote(conn, symbol_id, data["quote"])

            conn.commit()
            logger.info(f"Successfully processed and committed data for {symbol_ticker}")

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Transaction failed for {symbol_ticker}: {e}")
        finally:
            if conn:
                conn.close()

    except Exception as e:
        logger.error(f"Failed to process symbol {symbol_ticker}: {e}")


def main():
    """Main function to run the update script."""
    logger.info("Starting stock info update script.")

    symbols = get_all_symbols()
    if not symbols:
        logger.warning("No symbols found in SymbolsMaster. Exiting.")
        return

    logger.info(f"Found {len(symbols)} symbols to process.")

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_symbol, symbol) for symbol in symbols]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"A thread generated an exception: {e}")

    logger.info("Stock info update script finished.")


if __name__ == "__main__":
    main()
