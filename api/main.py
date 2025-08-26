import asyncio
import os
import logging
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
import concurrent.futures
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Security
from psycopg2.extras import execute_values
from psycopg2.extensions import connection as PgConnection
from fastapi.security import APIKeyHeader

from .database import get_db_connection
from .fetcher import fetch_stock_data
from .models import StockPrice, SymbolsMaster, SymbolsMeta, SymbolQuote
from cuid2 import Cuid

# 1. Set up logging
log_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Log to a rotating file
file_handler = RotatingFileHandler(
    "microservice.log", maxBytes=5 * 1024 * 1024, backupCount=3
)
file_handler.setFormatter(log_formatter)
file_handler.setLevel(logging.INFO)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

# Configure the root logger
root_logger = logging.getLogger()
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)
root_logger.setLevel(logging.INFO)

# Get a logger for this specific module
logger = logging.getLogger(__name__)

# Create a scheduler instance
scheduler = AsyncIOScheduler()
cuid_generator = Cuid()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Schedule the EOD fetch job to run every weekday at 22:00 UTC.
    # For testing, you could use a shorter interval like:
    # scheduler.add_job(fetch_and_store_eod_data, "interval", seconds=60)
    scheduler.add_job(
        fetch_and_store_eod_data,
        "cron",
        day_of_week="mon-fri",
        hour=22,
        minute=0,
        timezone="utc",
        id="eod_fetch_job",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started. EOD fetch job scheduled.")
    yield
    scheduler.shutdown()
    logger.info("Scheduler shut down.")


API_KEY = os.getenv("API_KEY")
API_KEY_NAME = "X-API-Key"

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

app = FastAPI(lifespan=lifespan)

async def get_api_key(key: str = Security(api_key_header)):
    if not API_KEY:
        raise HTTPException(status_code=500, detail="API_KEY is not configured on the server.")
    if key == API_KEY:
        return key
    else:
        raise HTTPException(status_code=403, detail="Could not validate credentials.")

from .database import get_db


@app.get("/")
async def health_check():
    return {"status": "healthy"}

def fetch_and_store_eod_data():
    """
    This is the core logic that will run in the background.
    It needs its own database connection because it runs outside the
    request/response cycle of the Depends(get_db) dependency.
    """
    logger.info("Background task: Starting EOD data fetch.")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Get top 500 stocks from your SymbolsMaster table
            cursor.execute('SELECT symbol FROM "SymbolsMaster" LIMIT 500')
            symbols = list(set([row[0] for row in cursor.fetchall()]))

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(fetch_stock_data, symbols))

        valid_results = [res for res in results if res]
        if not valid_results:
            logger.info("Background task: No new data to fetch or insert.")
            return

        with conn.cursor() as cursor:
            # Upsert data for SymbolsMeta
            meta_data_tuples = [
                (
                    cuid_generator.generate(),
                    res['meta']['symbol'],
                    res['meta']['sector'],
                    res['meta']['industry'],
                    res['meta']['marketCap'],
                    res['meta']['dividendYield'],
                    res['meta']['debtEq'],
                    res['meta']['rOE'],
                    res['meta']['website'],
                    res['meta']['country'],
                    res['meta']['description'],
                    res['meta']['logo'],
                    res['meta']['source'],
                    res['meta']['lastUpdated'],
                )
                for res in valid_results if res.get('meta')
            ]
            if meta_data_tuples:
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
                execute_values(cursor, insert_meta_query, meta_data_tuples)

            # Upsert data for SymbolQuote
            quote_data_tuples = [
                (
                    cuid_generator.generate(),
                    res['quote']['symbol'],
                    res['quote']['price'],
                    res['quote']['change'],
                    res['quote']['volume'],
                    res['quote']['pE'],
                    res['quote']['pEG'],
                    res['quote']['pB'],
                    res['quote']['beta'],
                    res['quote']['w52High'],
                    res['quote']['w52Low'],
                    res['quote']['recommendation'],
                    res['quote']['lastUpdated'],
                )
                for res in valid_results if res.get('quote')
            ]
            if quote_data_tuples:
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
                execute_values(cursor, insert_quote_query, quote_data_tuples)

            # Insert data for StockPrice
            price_data_tuples = [
                (
                    cuid_generator.generate(),
                    res['price']['symbol'],
                    res['price']['date'],
                    res['price']['open'],
                    res['price']['high'],
                    res['price']['low'],
                    res['price']['close'],
                    res['price']['volume'],
                )
                for res in valid_results if res.get('price')
            ]
            if price_data_tuples:
                insert_price_query = """
                    INSERT INTO "StockPrice" (id, symbol, date, open, high, low, close, volume)
                    VALUES %s ON CONFLICT (symbol, date) DO NOTHING
                """
                execute_values(cursor, insert_price_query, price_data_tuples)

            conn.commit()
        logger.info(f"Background task: Successfully processed {len(valid_results)} records.")
    except Exception as e:
        logger.error("Background task failed", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

@app.post("/trigger-eod-fetch", dependencies=[Depends(get_api_key)])
async def trigger_eod_fetch(
    background_tasks: BackgroundTasks,
):
    try:
        background_tasks.add_task(fetch_and_store_eod_data)
        return {"message": "EOD data fetch has been triggered in the background."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {e}")

@app.get(
    "/stock/{symbol}",
    response_model=StockPrice,
    dependencies=[Depends(get_api_key)],
)
async def get_latest_stock_price(symbol: str, conn: PgConnection = Depends(get_db)):
    """
    Retrieves the most recent end-of-day stock price for a given symbol.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT symbol, date, open, high, low, close, volume
            FROM "StockPrice"
            WHERE symbol = %s
            ORDER BY date DESC
            LIMIT 1
            """,
            (symbol.upper(),),  # Normalize symbol to uppercase
        )
        record = cursor.fetchone()

    if not record:
        raise HTTPException(status_code=404, detail=f"No stock price data found for symbol {symbol}")

    return StockPrice(**record)