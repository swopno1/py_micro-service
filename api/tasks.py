import logging
import requests
import concurrent.futures
from cuid2 import Cuid
from psycopg2.extras import execute_values
from .database import get_db_connection
from .fetcher import fetch_symbol_meta, fetch_symbol_quote, fetch_stock_price_history

logger = logging.getLogger(__name__)
cuid_generator = Cuid()
FAILURE_THRESHOLD = 5

# --- Symbol & Status Management ---

def _get_stock_symbols():
    """Fetches stock symbols from various sources."""
    # ... (implementation is unchanged)
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
    # ... (implementation is unchanged)
    logger.info("Fetching crypto symbols from Coinbase.")
    symbols = [
        {"symbol": "BTC-USD", "name": "Bitcoin", "type": "crypto", "source": "YAHOO"},
        {"symbol": "ETH-USD", "name": "Ethereum", "type": "crypto", "source": "YAHOO"},
    ]
    logger.info(f"Successfully fetched {len(symbols)} crypto symbols.")
    return symbols

def _fetch_active_symbols_with_attempts(limit=None):
    """Fetches active symbols and their failure counts."""
    symbols_data = []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            query = 'SELECT symbol, "failedAttempts" FROM "SymbolsMaster" WHERE status = %s'
            params = ['active']
            if limit:
                query += ' LIMIT %s'
                params.append(limit)
            cursor.execute(query, tuple(params))
            symbols_data = [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error("Failed to fetch active symbols.", exc_info=True)
    finally:
        if conn: conn.close()
    return symbols_data

def _update_symbol_statuses(cursor, symbol_map):
    """Bulk updates symbol statuses and failure counts based on fetch results."""
    reset_list = [s for s, data in symbol_map.items() if data['status'] == 'ok' and data['failedAttempts'] > 0]
    delist_list = [s for s, data in symbol_map.items() if data['status'] == 'delisted']
    suspend_list = [s for s, data in symbol_map.items() if data['status'] == 'suspended']

    # Increment failures for transient errors
    increment_list = [s for s, data in symbol_map.items() if data['status'] == 'fetch_failed']
    if increment_list:
        logger.info(f"Incrementing failure count for {len(increment_list)} symbols.")
        cursor.execute('UPDATE "SymbolsMaster" SET "failedAttempts" = "failedAttempts" + 1, "lastUpdated" = NOW() WHERE symbol = ANY(%s);', (increment_list,))

    # Reset failure count for successful fetches
    if reset_list:
        logger.info(f"Resetting failure count for {len(reset_list)} symbols.")
        cursor.execute('UPDATE "SymbolsMaster" SET "failedAttempts" = 0, "lastUpdated" = NOW() WHERE symbol = ANY(%s);', (reset_list,))

    # Update statuses for delisted and suspended symbols
    if delist_list:
        logger.info(f"Updating status to 'delisted' for {len(delist_list)} symbols.")
        cursor.execute('UPDATE "SymbolsMaster" SET status = %s, "lastUpdated" = NOW() WHERE symbol = ANY(%s);', ('delisted', delist_list))
    if suspend_list:
        logger.info(f"Updating status to 'suspended' for {len(suspend_list)} symbols.")
        cursor.execute('UPDATE "SymbolsMaster" SET status = %s, "lastUpdated" = NOW() WHERE symbol = ANY(%s);', ('suspended', suspend_list))

# --- Main Task Functions ---

def update_symbols_master():
    """Populates and updates the SymbolsMaster table."""
    # ... (implementation is largely unchanged, but ensures status is reset)
    logger.info("Starting SymbolsMaster update task.")
    all_symbols = _get_stock_symbols() + _get_crypto_symbols()
    if not all_symbols:
        logger.warning("No symbols were fetched. Aborting update task.")
        return
    data_tuples = [(cuid_generator.generate(), s['symbol'], s['name'], s['source'], s['type'], 'active', 0, 'NOW()') for s in all_symbols]
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO "SymbolsMaster" (id, symbol, name, source, type, status, "failedAttempts", "lastUpdated")
                VALUES %s ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name, source = EXCLUDED.source,
                    type = EXCLUDED.type, status = 'active', -- Always reset to active
                    "failedAttempts" = 0, -- Always reset attempts
                    "lastUpdated" = EXCLUDED."lastUpdated";
            """
            execute_values(cursor, insert_query, data_tuples)
            conn.commit()
        logger.info(f"Successfully upserted {len(data_tuples)} records into SymbolsMaster.")
    except Exception as e:
        logger.error("Failed to update SymbolsMaster.", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def _process_task(task_name, fetch_function, write_function, limit=None):
    """Generic function to handle the fetch-process-write cycle for tasks."""
    logger.info(f"Starting {task_name} task.")
    symbols_data = _fetch_active_symbols_with_attempts(limit=limit)
    if not symbols_data:
        logger.info(f"No active symbols to process for {task_name}.")
        return
    logger.info(f"Found {len(symbols_data)} active symbols for {task_name}.")

    symbol_map = {s['symbol']: s for s in symbols_data}
    symbols_list = list(symbol_map.keys())

    valid_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_function, symbols_list))

    for i, (status, data) in enumerate(results):
        symbol = symbols_list[i]
        symbol_map[symbol]['status'] = status
        if status == 'ok':
            valid_results.append(data)
        elif status == 'fetch_failed':
            symbol_map[symbol]['failedAttempts'] += 1
            if symbol_map[symbol]['failedAttempts'] >= FAILURE_THRESHOLD:
                symbol_map[symbol]['status'] = 'suspended'

    if not valid_results and all(v['status'] == 'ok' for v in symbol_map.values()):
        logger.info(f"No data fetched and no status changes for {task_name}.")
        return

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            _update_symbol_statuses(cursor, symbol_map)
            if valid_results:
                write_function(cursor, valid_results)
            conn.commit()
        logger.info(f"{task_name} task database transaction committed.")
    except Exception as e:
        logger.error(f"Failed to write {task_name} updates to DB.", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def update_symbols_meta():
    def write_meta(cursor, results):
        data_tuples = [(cuid_generator.generate(), r['symbol'], r['sector'], r['industry'], r['marketCap'], r['dividendYield'], r['debtEq'], r['rOE'], r['website'], r['country'], r['description'], r['logo'], r['source'], r['lastUpdated']) for r in results]
        insert_query = """
            INSERT INTO "SymbolsMeta" (id, symbol, sector, industry, "marketCap", "dividendYield", "debtEq", "rOE", website, country, description, logo, source, "lastUpdated")
            VALUES %s ON CONFLICT (symbol) DO UPDATE SET
                sector = EXCLUDED.sector, industry = EXCLUDED.industry, "marketCap" = EXCLUDED."marketCap", "dividendYield" = EXCLUDED."dividendYield", "debtEq" = EXCLUDED."debtEq", "rOE" = EXCLUDED."rOE", website = EXCLUDED.website, country = EXCLUDED.country, description = EXCLUDED.description, logo = EXCLUDED.logo, source = EXCLUDED.source, "lastUpdated" = EXCLUDED."lastUpdated";
        """
        execute_values(cursor, insert_query, data_tuples)
        logger.info(f"Queued upsert for {len(results)} meta records.")
    _process_task("SymbolsMeta", fetch_symbol_meta, write_meta)

def update_symbol_quotes():
    def write_quotes(cursor, results):
        data_tuples = [(cuid_generator.generate(), r['symbol'], r['price'], r['change'], r['volume'], r['pE'], r['pEG'], r['pB'], r['beta'], r['w52High'], r['w52Low'], r['recommendation'], r['lastUpdated']) for r in results]
        insert_query = """
            INSERT INTO "SymbolQuote" (id, symbol, price, change, volume, "pE", "pEG", "pB", beta, "w52High", "w52Low", recommendation, "lastUpdated")
            VALUES %s ON CONFLICT (symbol) DO UPDATE SET
                price = EXCLUDED.price, change = EXCLUDED.change, volume = EXCLUDED.volume, "pE" = EXCLUDED."pE", "pEG" = EXCLUDED."pEG", "pB" = EXCLUDED."pB", beta = EXCLUDED.beta, "w52High" = EXCLUDED."w52High", "w52Low" = EXCLUDED."w52Low", recommendation = EXCLUDED.recommendation, "lastUpdated" = EXCLUDED."lastUpdated";
        """
        execute_values(cursor, insert_query, data_tuples)
        logger.info(f"Queued upsert for {len(results)} quote records.")
    _process_task("SymbolQuote", fetch_symbol_quote, write_quotes, limit=500)

def update_stock_prices():
    def write_prices(cursor, results):
        all_records = [record for sublist in results for record in sublist]
        if not all_records: return
        data_tuples = [(cuid_generator.generate(), r['symbol'], r['date'], r['open'], r['high'], r['low'], r['close'], r['volume']) for r in all_records]
        insert_query = 'INSERT INTO "StockPrice" (id, symbol, date, open, high, low, close, volume) VALUES %s ON CONFLICT (symbol, date) DO NOTHING;'
        execute_values(cursor, insert_query, data_tuples)
        logger.info(f"Queued insert for {len(data_tuples)} price records.")
    _process_task("StockPrice", fetch_stock_price_history, write_prices, limit=500)

async def update_symbol_from_external_source(symbol: str, conn):
    """
    Fetches and updates data for a single symbol from an external source
    and returns the updated data.
    """
    import asyncio
    import concurrent.futures
    from .fetcher import fetch_symbol_meta, fetch_symbol_quote, fetch_stock_price_history
    from .models import SymbolsMeta, SymbolQuote, StockPrice

    loop = asyncio.get_event_loop()
    updated_data = {"meta": None, "quote": None, "price_history": []}

    with concurrent.futures.ThreadPoolExecutor() as pool:
        # Fetch data in parallel
        meta_future = loop.run_in_executor(pool, fetch_symbol_meta, symbol)
        quote_future = loop.run_in_executor(pool, fetch_symbol_quote, symbol)
        price_future = loop.run_in_executor(pool, fetch_stock_price_history, symbol)

        meta_status, meta_data = await meta_future
        quote_status, quote_data = await quote_future
        price_status, price_data = await price_future

    try:
        was_anything_updated = False
        with conn.cursor() as cursor:
            # Upsert meta data
            if meta_status == 'ok' and meta_data:
                was_anything_updated = True
                meta_tuple = (
                    cuid_generator.generate(), meta_data['symbol'], meta_data['sector'], meta_data['industry'],
                    meta_data['marketCap'], meta_data['dividendYield'], meta_data['debtEq'], meta_data['rOE'],
                    meta_data['website'], meta_data['country'], meta_data['description'], meta_data['logo'],
                    meta_data['source'], meta_data['lastUpdated']
                )
                meta_query = """
                    INSERT INTO "SymbolsMeta" (id, symbol, sector, industry, "marketCap", "dividendYield", "debtEq", "rOE", website, country, description, logo, source, "lastUpdated")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET
                        sector = EXCLUDED.sector, industry = EXCLUDED.industry, "marketCap" = EXCLUDED."marketCap",
                        "dividendYield" = EXCLUDED."dividendYield", "debtEq" = EXCLUDED."debtEq", "rOE" = EXCLUDED."rOE",
                        website = EXCLUDED.website, country = EXCLUDED.country, description = EXCLUDED.description,
                        logo = EXCLUDED.logo, source = EXCLUDED.source, "lastUpdated" = EXCLUDED."lastUpdated"
                    RETURNING *;
                """
                cursor.execute(meta_query, meta_tuple)
                record = cursor.fetchone()
                if record:
                    colnames = [desc[0] for desc in cursor.description]
                    updated_data["meta"] = SymbolsMeta(**dict(zip(colnames, record)))

            # Upsert quote data
            if quote_status == 'ok' and quote_data:
                was_anything_updated = True
                quote_tuple = (
                    cuid_generator.generate(), quote_data['symbol'], quote_data['price'], quote_data['change'],
                    quote_data['volume'], quote_data['pE'], quote_data['pEG'], quote_data['pB'], quote_data['beta'],
                    quote_data['w52High'], quote_data['w52Low'], quote_data['recommendation'], quote_data['lastUpdated']
                )
                quote_query = """
                    INSERT INTO "SymbolQuote" (id, symbol, price, change, volume, "pE", "pEG", "pB", beta, "w52High", "w52Low", recommendation, "lastUpdated")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET
                        price = EXCLUDED.price, change = EXCLUDED.change, volume = EXCLUDED.volume, "pE" = EXCLUDED."pE",
                        "pEG" = EXCLUDED."pEG", "pB" = EXCLUDED."pB", beta = EXCLUDED.beta, "w52High" = EXCLUDED."w52High",
                        "w52Low" = EXCLUDED."w52Low", recommendation = EXCLUDED.recommendation, "lastUpdated" = EXCLUDED."lastUpdated"
                    RETURNING *;
                """
                cursor.execute(quote_query, quote_tuple)
                record = cursor.fetchone()
                if record:
                    colnames = [desc[0] for desc in cursor.description]
                    updated_data["quote"] = SymbolQuote(**dict(zip(colnames, record)))

            # Insert stock price data
            if price_status == 'ok' and price_data:
                price_tuples = [
                    (cuid_generator.generate(), r['symbol'], r['date'], r['open'], r['high'], r['low'], r['close'], r['volume'])
                    for r in price_data
                ]
                if price_tuples:
                    was_anything_updated = True
                    price_query = 'INSERT INTO "StockPrice" (id, symbol, date, open, high, low, close, volume) VALUES %s ON CONFLICT (symbol, date) DO NOTHING RETURNING symbol, date, open, high, low, close, volume;'
                    execute_values(cursor, price_query, price_tuples, fetch=True)
                    records = cursor.fetchall()
                    colnames = [desc[0] for desc in cursor.description]
                    updated_data["price_history"] = [StockPrice(**dict(zip(colnames, r))) for r in records]

        conn.commit()
        if was_anything_updated:
            logger.info(f"Successfully updated data for symbol {symbol}.")
        else:
            logger.warning(
                f"No new data was fetched or updated for symbol {symbol}. "
                f"Fetch status: meta='{meta_status}', quote='{quote_status}', price='{price_status}'."
            )

    except Exception as e:
        logger.error(f"Failed to update data for symbol {symbol}.", exc_info=True)
        if conn:
            conn.rollback()
        # Reraise the exception to be handled by the endpoint
        raise

    return updated_data


if __name__ == '__main__':
    update_symbols_master()
