import logging
import requests
import concurrent.futures
from cuid2 import Cuid
from psycopg2.extras import execute_values
from .database import get_db_connection
from .fetcher import fetch_symbol_meta, fetch_symbol_quote, fetch_stock_price_history

logger = logging.getLogger(__name__)
cuid_generator = Cuid()

def _get_stock_symbols():
    """
    Fetches stock symbols from various sources.
    TODO: Implement NASDAQ Data Link for a more comprehensive list.
    """
    logger.info("Fetching S&P 500 symbols as a source for stock symbols.")
    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    symbols = []
    try:
        response = requests.get(url)
        response.raise_for_status()
        lines = response.text.strip().split('\n')[1:]
        for line in lines:
            parts = line.split(',')
            if len(parts) >= 2:
                # The data is from yfinance, so the source should be YAHOO
                symbols.append({
                    "symbol": parts[0].strip(),
                    "name": parts[1].strip(),
                    "type": "stock",
                    "source": "YAHOO"
                })
        logger.info(f"Successfully fetched {len(symbols)} stock symbols.")
        return symbols
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching stock symbols: {e}")
        return []

def _get_crypto_symbols():
    """
    Fetches crypto symbols from Coinbase.
    """
    logger.info("Fetching crypto symbols from Coinbase.")
    # TODO: Implement Coinbase /products endpoint logic
    # url = "https://api.exchange.coinbase.com/products"
    # For now, returning a sample list
    # yfinance can fetch crypto data, so we can treat it as the same source for now.
    symbols = [
        {"symbol": "BTC-USD", "name": "Bitcoin", "type": "crypto", "source": "YAHOO"},
        {"symbol": "ETH-USD", "name": "Ethereum", "type": "crypto", "source": "YAHOO"},
    ]
    logger.info(f"Successfully fetched {len(symbols)} crypto symbols.")
    return symbols

def update_symbols_master():
    """
    The core task for populating and updating the SymbolsMaster table.
    It fetches symbols from all available sources and upserts them.
    """
    logger.info("Starting SymbolsMaster update task.")

    # 1. Fetch symbols from all sources
    stock_symbols = _get_stock_symbols()
    crypto_symbols = _get_crypto_symbols()
    all_symbols = stock_symbols + crypto_symbols

    if not all_symbols:
        logger.warning("No symbols were fetched. Aborting update task.")
        return

    # 2. Prepare data for database insertion
    data_tuples = [
        (
            cuid_generator.generate(),
            s['symbol'],
            s['name'],
            s['source'],
            s['type'],
            'active', # Assuming all fetched symbols are active
            'NOW()'
        )
        for s in all_symbols
    ]

    # 3. Upsert into the database
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO "SymbolsMaster" (id, symbol, name, source, type, status, "lastUpdated")
                VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name,
                    source = EXCLUDED.source,
                    type = EXCLUDED.type,
                    status = EXCLUDED.status,
                    "lastUpdated" = EXCLUDED."lastUpdated";
            """
            execute_values(cursor, insert_query, data_tuples)
            conn.commit()
        logger.info(f"Successfully upserted {len(data_tuples)} records into SymbolsMaster.")
    except Exception as e:
        logger.error("Failed to update SymbolsMaster.", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def update_symbols_meta():
    """
    Fetches and updates the fundamental data for all symbols in the SymbolsMaster table.
    """
    logger.info("Starting SymbolsMeta update task.")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT symbol FROM "SymbolsMaster"')
            # Using a set to ensure unique symbols
            symbols = list(set([row[0] for row in cursor.fetchall()]))

        logger.info(f"Found {len(symbols)} symbols to update meta for.")

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(fetch_symbol_meta, symbols))

        valid_results = [res for res in results if res]
        if not valid_results:
            logger.info("No new meta data fetched. Aborting update task.")
            return

        data_tuples = [
            (
                cuid_generator.generate(),
                res['symbol'],
                res['sector'],
                res['industry'],
                res['marketCap'],
                res['dividendYield'],
                res['debtEq'],
                res['rOE'],
                res['website'],
                res['country'],
                res['description'],
                res['logo'],
                res['source'],
                res['lastUpdated'],
            )
            for res in valid_results
        ]

        with conn.cursor() as cursor:
            insert_meta_query = """
                INSERT INTO "SymbolsMeta" (id, symbol, sector, industry, "marketCap", "dividendYield", "debtEq", "rOE", website, country, description, logo, source, "lastUpdated")
                VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    "marketCap" = EXCLUDED."marketCap",
                    "dividendYield" = EXCLUDED."dividendYield",
                    "debtEq" = EXCLUDED."debtEq",
                    "rOE" = EXCLUDED."rOE",
                    website = EXCLUDED.website,
                    country = EXCLUDED.country,
                    description = EXCLUDED.description,
                    logo = EXCLUDED.logo,
                    source = EXCLUDED.source,
                    "lastUpdated" = EXCLUDED."lastUpdated";
            """
            execute_values(cursor, insert_meta_query, data_tuples)
            conn.commit()
        logger.info(f"Successfully upserted meta data for {len(valid_results)} symbols.")

    except Exception as e:
        logger.error("Failed to update SymbolsMeta.", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def update_symbol_quotes():
    """
    Fetches and updates the quote data for all symbols in the SymbolsMaster table.
    """
    logger.info("Starting SymbolQuote update task.")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT symbol FROM "SymbolsMaster" LIMIT 500') # Limiting for performance
            symbols = list(set([row[0] for row in cursor.fetchall()]))

        logger.info(f"Found {len(symbols)} symbols to update quotes for.")

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(fetch_symbol_quote, symbols))

        valid_results = [res for res in results if res]
        if not valid_results:
            logger.info("No new quote data fetched. Aborting update task.")
            return

        data_tuples = [
            (
                cuid_generator.generate(),
                res['symbol'],
                res['price'],
                res['change'],
                res['volume'],
                res['pE'],
                res['pEG'],
                res['pB'],
                res['beta'],
                res['w52High'],
                res['w52Low'],
                res['recommendation'],
                res['lastUpdated'],
            )
            for res in valid_results
        ]

        with conn.cursor() as cursor:
            insert_quote_query = """
                INSERT INTO "SymbolQuote" (id, symbol, price, change, volume, "pE", "pEG", "pB", beta, "w52High", "w52Low", recommendation, "lastUpdated")
                VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                    price = EXCLUDED.price,
                    change = EXCLUDED.change,
                    volume = EXCLUDED.volume,
                    "pE" = EXCLUDED."pE",
                    "pEG" = EXCLUDED."pEG",
                    "pB" = EXCLUDED."pB",
                    beta = EXCLUDED.beta,
                    "w52High" = EXCLUDED."w52High",
                    "w52Low" = EXCLUDED."w52Low",
                    recommendation = EXCLUDED.recommendation,
                    "lastUpdated" = EXCLUDED."lastUpdated";
            """
            execute_values(cursor, insert_quote_query, data_tuples)
            conn.commit()
        logger.info(f"Successfully upserted quote data for {len(valid_results)} symbols.")

    except Exception as e:
        logger.error("Failed to update SymbolQuote.", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def update_stock_prices():
    """
    Fetches and inserts the latest daily OHLCV data for all symbols.
    """
    logger.info("Starting StockPrice update task.")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT symbol FROM "SymbolsMaster" LIMIT 500') # Limiting for performance
            symbols = list(set([row[0] for row in cursor.fetchall()]))

        logger.info(f"Found {len(symbols)} symbols to update stock prices for.")

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # This returns a list of lists, where each inner list has daily records
            results = list(executor.map(fetch_stock_price_history, symbols))

        # Flatten the list of lists into a single list of records
        all_price_records = [record for sublist in results for record in sublist]

        if not all_price_records:
            logger.info("No new price data fetched. Aborting update task.")
            return

        data_tuples = [
            (
                cuid_generator.generate(),
                rec['symbol'],
                rec['date'],
                rec['open'],
                rec['high'],
                rec['low'],
                rec['close'],
                rec['volume'],
            )
            for rec in all_price_records
        ]

        with conn.cursor() as cursor:
            insert_price_query = """
                INSERT INTO "StockPrice" (id, symbol, date, open, high, low, close, volume)
                VALUES %s ON CONFLICT (symbol, date) DO NOTHING;
            """
            execute_values(cursor, insert_price_query, data_tuples)
            conn.commit()
        logger.info(f"Successfully inserted {len(data_tuples)} new price records.")

    except Exception as e:
        logger.error("Failed to update StockPrice.", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    # This allows running the task directly for testing
    update_symbols_master()
