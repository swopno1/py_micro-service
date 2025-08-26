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
    """Fetches stock symbols from various sources."""
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
                symbols.append({
                    "symbol": parts[0].strip(), "name": parts[1].strip(),
                    "type": "stock", "source": "YAHOO"
                })
        logger.info(f"Successfully fetched {len(symbols)} stock symbols.")
        return symbols
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching stock symbols: {e}")
        return []

def _get_crypto_symbols():
    """Fetches crypto symbols from Coinbase."""
    logger.info("Fetching crypto symbols from Coinbase.")
    symbols = [
        {"symbol": "BTC-USD", "name": "Bitcoin", "type": "crypto", "source": "YAHOO"},
        {"symbol": "ETH-USD", "name": "Ethereum", "type": "crypto", "source": "YAHOO"},
    ]
    logger.info(f"Successfully fetched {len(symbols)} crypto symbols.")
    return symbols

def _update_symbol_status(cursor, symbols_to_update, new_status):
    """
    Helper function to bulk update symbol statuses using an existing cursor.
    This does not commit the transaction.
    """
    if not symbols_to_update:
        return

    logger.info(f"Queueing status update to '{new_status}' for {len(symbols_to_update)} symbols.")
    update_query = """
        UPDATE "SymbolsMaster" SET status = %s, "lastUpdated" = NOW()
        WHERE symbol = ANY(%s);
    """
    cursor.execute(update_query, (new_status, symbols_to_update))

def update_symbols_master():
    """Populates and updates the SymbolsMaster table."""
    logger.info("Starting SymbolsMaster update task.")
    all_symbols = _get_stock_symbols() + _get_crypto_symbols()
    if not all_symbols:
        logger.warning("No symbols were fetched. Aborting update task.")
        return

    data_tuples = [
        (cuid_generator.generate(), s['symbol'], s['name'], s['source'], s['type'], 'active', 'NOW()')
        for s in all_symbols
    ]
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO "SymbolsMaster" (id, symbol, name, source, type, status, "lastUpdated")
                VALUES %s ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name, source = EXCLUDED.source,
                    type = EXCLUDED.type, status = 'active', -- Always reset to active on master update
                    "lastUpdated" = EXCLUDED."lastUpdated";
            """
            execute_values(cursor, insert_query, data_tuples)
            conn.commit() # This task is standalone, so it commits its own transaction.
        logger.info(f"Successfully upserted {len(data_tuples)} records into SymbolsMaster.")
    except Exception as e:
        logger.error("Failed to update SymbolsMaster.", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def update_symbols_meta():
    """Fetches and updates fundamental data for active symbols."""
    logger.info("Starting SymbolsMeta update task.")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT symbol FROM "SymbolsMaster" WHERE status = %s', ('active',))
            symbols = list(set([row[0] for row in cursor.fetchall()]))

            if not symbols:
                logger.info("No active symbols to update meta for.")
                return
            logger.info(f"Found {len(symbols)} active symbols to update meta for.")

            symbols_to_delist, symbols_to_fail, valid_results = [], [], []
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(fetch_symbol_meta, symbols))

            for i, (status, data) in enumerate(results):
                if status == 'ok': valid_results.append(data)
                elif status == 'delisted': symbols_to_delist.append(symbols[i])
                else: symbols_to_fail.append(symbols[i])

            # Queue status updates
            _update_symbol_status(cursor, symbols_to_delist, 'delisted')
            _update_symbol_status(cursor, symbols_to_fail, 'fetch_failed')

            if valid_results:
                data_tuples = [(cuid_generator.generate(), r['symbol'], r['sector'], r['industry'], r['marketCap'], r['dividendYield'], r['debtEq'], r['rOE'], r['website'], r['country'], r['description'], r['logo'], r['source'], r['lastUpdated']) for r in valid_results]
                insert_query = """
                    INSERT INTO "SymbolsMeta" (id, symbol, sector, industry, "marketCap", "dividendYield", "debtEq", "rOE", website, country, description, logo, source, "lastUpdated")
                    VALUES %s ON CONFLICT (symbol) DO UPDATE SET
                        sector = EXCLUDED.sector, industry = EXCLUDED.industry, "marketCap" = EXCLUDED."marketCap", "dividendYield" = EXCLUDED."dividendYield", "debtEq" = EXCLUDED."debtEq", "rOE" = EXCLUDED."rOE", website = EXCLUDED.website, country = EXCLUDED.country, description = EXCLUDED.description, logo = EXCLUDED.logo, source = EXCLUDED.source, "lastUpdated" = EXCLUDED."lastUpdated";
                """
                execute_values(cursor, insert_query, data_tuples)
                logger.info(f"Successfully queued upsert for {len(valid_results)} meta records.")

            # Commit the entire transaction
            conn.commit()
            logger.info("SymbolsMeta task transaction committed successfully.")

    except Exception as e:
        logger.error("Failed to update SymbolsMeta.", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def update_symbol_quotes():
    """Fetches and updates quote data for active symbols."""
    logger.info("Starting SymbolQuote update task.")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT symbol FROM "SymbolsMaster" WHERE status = %s LIMIT 500', ('active',))
            symbols = list(set([row[0] for row in cursor.fetchall()]))

            if not symbols:
                logger.info("No active symbols to update quotes for.")
                return
            logger.info(f"Found {len(symbols)} active symbols to update quotes for.")

            symbols_to_delist, symbols_to_fail, valid_results = [], [], []
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(fetch_symbol_quote, symbols))

            for i, (status, data) in enumerate(results):
                if status == 'ok': valid_results.append(data)
                elif status == 'delisted': symbols_to_delist.append(symbols[i])
                else: symbols_to_fail.append(symbols[i])

            _update_symbol_status(cursor, symbols_to_delist, 'delisted')
            _update_symbol_status(cursor, symbols_to_fail, 'fetch_failed')

            if valid_results:
                data_tuples = [(cuid_generator.generate(), r['symbol'], r['price'], r['change'], r['volume'], r['pE'], r['pEG'], r['pB'], r['beta'], r['w52High'], r['w52Low'], r['recommendation'], r['lastUpdated']) for r in valid_results]
                insert_query = """
                    INSERT INTO "SymbolQuote" (id, symbol, price, change, volume, "pE", "pEG", "pB", beta, "w52High", "w52Low", recommendation, "lastUpdated")
                    VALUES %s ON CONFLICT (symbol) DO UPDATE SET
                        price = EXCLUDED.price, change = EXCLUDED.change, volume = EXCLUDED.volume, "pE" = EXCLUDED."pE", "pEG" = EXCLUDED."pEG", "pB" = EXCLUDED."pB", beta = EXCLUDED.beta, "w52High" = EXCLUDED."w52High", "w52Low" = EXCLUDED."w52Low", recommendation = EXCLUDED.recommendation, "lastUpdated" = EXCLUDED."lastUpdated";
                """
                execute_values(cursor, insert_query, data_tuples)
                logger.info(f"Successfully queued upsert for {len(valid_results)} quote records.")

            conn.commit()
            logger.info("SymbolQuote task transaction committed successfully.")
    except Exception as e:
        logger.error("Failed to update SymbolQuote.", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def update_stock_prices():
    """Fetches and inserts the latest daily OHLCV data for active symbols."""
    logger.info("Starting StockPrice update task.")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT symbol FROM "SymbolsMaster" WHERE status = %s LIMIT 500', ('active',))
            symbols = list(set([row[0] for row in cursor.fetchall()]))

            if not symbols:
                logger.info("No active symbols to update stock prices for.")
                return
            logger.info(f"Found {len(symbols)} active symbols to update stock prices for.")

            symbols_to_delist, symbols_to_fail, all_price_records = [], [], []
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(fetch_stock_price_history, symbols))

            for i, (status, data) in enumerate(results):
                if status == 'ok': all_price_records.extend(data)
                elif status == 'delisted': symbols_to_delist.append(symbols[i])
                else: symbols_to_fail.append(symbols[i])

            _update_symbol_status(cursor, symbols_to_delist, 'delisted')
            _update_symbol_status(cursor, symbols_to_fail, 'fetch_failed')

            if all_price_records:
                data_tuples = [(cuid_generator.generate(), r['symbol'], r['date'], r['open'], r['high'], r['low'], r['close'], r['volume']) for r in all_price_records]
                insert_query = 'INSERT INTO "StockPrice" (id, symbol, date, open, high, low, close, volume) VALUES %s ON CONFLICT (symbol, date) DO NOTHING;'
                execute_values(cursor, insert_query, data_tuples)
                logger.info(f"Successfully queued insert for {len(data_tuples)} price records.")

            conn.commit()
            logger.info("StockPrice task transaction committed successfully.")
    except Exception as e:
        logger.error("Failed to update StockPrice.", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == '__main__':
    update_symbols_master()
