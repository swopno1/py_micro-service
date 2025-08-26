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

from .database import get_db_connection, get_db
from .fetcher import fetch_stock_data
from .models import StockPrice
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

def fetch_and_store_eod_data():
    """
    This is the core logic that will run in the background.
    It fetches and stores the end-of-day stock prices.
    """
    logger.info("Background task: Starting EOD data fetch.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute('SELECT symbol FROM "SymbolsMaster"')
            # Use a set to avoid duplicate symbols if any
            symbols = list(set([row['symbol'] for row in cursor.fetchall()]))

        if not symbols:
            logger.info("No symbols found in SymbolsMaster. Skipping EOD fetch.")
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(fetch_stock_data, symbols))

        valid_results = [res for res in results if res and res.get('price')]
        if not valid_results:
            logger.info("Background task: No new price data to fetch or insert.")
            return

        price_data_tuples = []
        for res in valid_results:
            price = res['price']
            price_data_tuples.append(
                (
                    cuid_generator.generate(),
                    price['symbol'],
                    price['date'],
                    price['open'],
                    price['high'],
                    price['low'],
                    price['close'],
                    price['volume'],
                )
            )

        if price_data_tuples:
            with conn.cursor() as cursor:
                insert_price_query = """
                    INSERT INTO "StockPrice" (id, symbol, date, open, high, low, close, volume)
                    VALUES %s ON CONFLICT (symbol, date) DO NOTHING
                """
                execute_values(cursor, insert_price_query, price_data_tuples)
                conn.commit()
            logger.info(f"Background task: Successfully processed and stored {len(price_data_tuples)} price records.")
        else:
            logger.info("Background task: No valid price data tuples to insert.")

    except Exception as e:
        logger.error("Background task failed", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

@app.post("/trigger-eod-fetch", dependencies=[Depends(get_api_key)])
async def trigger_eod_fetch(background_tasks: BackgroundTasks):
    background_tasks.add_task(fetch_and_store_eod_data)
    return {"message": "EOD data fetch has been triggered in the background."}

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
            (symbol.upper(),),
        )
        record = cursor.fetchone()

    if not record:
        raise HTTPException(status_code=404, detail=f"No stock price data found for symbol {symbol}")

    return StockPrice(**record)

@app.get("/")
async def health_check():
    return {"status": "healthy"}