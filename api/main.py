import asyncio
import os
import logging
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
import concurrent.futures
from datetime import date
from typing import List, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import APIRouter, FastAPI, Depends, HTTPException, BackgroundTasks, Security, Query
from psycopg2.extras import execute_values
from psycopg2.extensions import connection as PgConnection
from fastapi.security import APIKeyHeader

from .database import get_db_connection, get_db
from .fetcher import (
    fetch_symbol_meta,
    fetch_symbol_quote,
    fetch_stock_price_history,
)
from .models import StockPrice, SymbolsMaster, SymbolsMeta, SymbolQuote
from .tasks import update_symbols_master, update_symbols_meta, update_symbol_quotes, update_stock_prices
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
    # Schedule the SymbolsMaster update job to run on the 1st day of every month.
    scheduler.add_job(
        update_symbols_master,
        "cron",
        day=1,
        hour=10,
        minute=0,
        timezone="utc",
        id="update_symbols_master_job",
        replace_existing=True,
    )

    # Schedule the SymbolsMeta update job to run on the 2nd day of every month.
    scheduler.add_job(
        update_symbols_meta,
        "cron",
        day=2,
        hour=10,
        minute=0,
        timezone="utc",
        id="update_symbols_meta_job",
        replace_existing=True,
    )

    # Schedule the SymbolQuote update job to run every weekday at 22:00 UTC.
    scheduler.add_job(
        update_symbol_quotes,
        "cron",
        day_of_week="mon-fri",
        hour=22,
        minute=0,
        timezone="utc",
        id="update_symbol_quotes_job",
        replace_existing=True,
    )

    # Schedule the StockPrice update job to run every weekday at 23:00 UTC
    scheduler.add_job(
        update_stock_prices,
        "cron",
        day_of_week="mon-fri",
        hour=23,
        minute=0,
        timezone="utc",
        id="update_stock_prices_job",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("Scheduler started. Jobs are scheduled.")
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

# Create a new router for task-related endpoints
tasks_router = APIRouter(prefix="/tasks", tags=["tasks"])

@tasks_router.post("/symbols/master/run", dependencies=[Depends(get_api_key)])
async def run_update_symbols_master(background_tasks: BackgroundTasks):
    """Manually triggers the SymbolsMaster update task."""
    background_tasks.add_task(update_symbols_master)
    return {"message": "SymbolsMaster update task has been triggered in the background."}

@tasks_router.post("/symbols/meta/run", dependencies=[Depends(get_api_key)])
async def run_update_symbols_meta(background_tasks: BackgroundTasks):
    """Manually triggers the SymbolsMeta update task."""
    background_tasks.add_task(update_symbols_meta)
    return {"message": "SymbolsMeta update task has been triggered in the background."}

@tasks_router.post("/symbols/quote/run", dependencies=[Depends(get_api_key)])
async def run_update_symbol_quotes(background_tasks: BackgroundTasks):
    """Manually triggers the SymbolQuote update task."""
    background_tasks.add_task(update_symbol_quotes)
    return {"message": "SymbolQuote update task has been triggered in the background."}

@tasks_router.post("/stocks/price/run", dependencies=[Depends(get_api_key)])
async def run_update_stock_prices(background_tasks: BackgroundTasks):
    """Manually triggers the StockPrice update task."""
    background_tasks.add_task(update_stock_prices)
    return {"message": "StockPrice update task has been triggered in the background."}

app.include_router(tasks_router)

# Create a new router for data retrieval endpoints
symbols_router = APIRouter(prefix="/symbols", tags=["symbols"])

@symbols_router.get(
    "/meta/{symbol}",
    response_model=SymbolsMeta,
    dependencies=[Depends(get_api_key)],
)
async def get_symbol_meta(symbol: str, conn: PgConnection = Depends(get_db)):
    """Retrieves the metadata for a given symbol."""
    with conn.cursor() as cursor:
        cursor.execute(
            'SELECT * FROM "SymbolsMeta" WHERE symbol = %s',
            (symbol.upper(),),
        )
        record = cursor.fetchone()

    if not record:
        raise HTTPException(status_code=404, detail=f"No metadata found for symbol {symbol}")

    # Fetch column names from cursor.description
    colnames = [desc[0] for desc in cursor.description]
    record_dict = dict(zip(colnames, record))

    return SymbolsMeta(**record_dict)

@symbols_router.get(
    "/quote/{symbol}",
    response_model=SymbolQuote,
    dependencies=[Depends(get_api_key)],
)
async def get_symbol_quote(symbol: str, conn: PgConnection = Depends(get_db)):
    """Retrieves the latest quote for a given symbol."""
    with conn.cursor() as cursor:
        cursor.execute(
            'SELECT * FROM "SymbolQuote" WHERE symbol = %s',
            (symbol.upper(),),
        )
        record = cursor.fetchone()

    if not record:
        raise HTTPException(status_code=404, detail=f"No quote data found for symbol {symbol}")

    colnames = [desc[0] for desc in cursor.description]
    record_dict = dict(zip(colnames, record))

    return SymbolQuote(**record_dict)

@symbols_router.get(
    "/stocks/{symbol}/price",
    response_model=List[StockPrice],
    dependencies=[Depends(get_api_key)],
)
async def get_stock_price_history(
    symbol: str,
    start_date: Optional[date] = Query(None, description="Start date for historical data (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date for historical data (YYYY-MM-DD)"),
    conn: PgConnection = Depends(get_db)
):
    """
    Retrieves the historical OHLCV data for a given symbol.
    """
    query = 'SELECT symbol, date, open, high, low, close, volume FROM "StockPrice" WHERE symbol = %s'
    params = [symbol.upper()]

    if start_date:
        query += " AND date >= %s"
        params.append(start_date)
    if end_date:
        query += " AND date <= %s"
        params.append(end_date)

    query += " ORDER BY date DESC"

    with conn.cursor() as cursor:
        cursor.execute(query, tuple(params))
        records = cursor.fetchall()

    if not records:
        raise HTTPException(status_code=404, detail=f"No stock price data found for symbol {symbol}")

    colnames = [desc[0] for desc in cursor.description]
    return [StockPrice(**dict(zip(colnames, record))) for record in records]

app.include_router(symbols_router)

@app.get("/")
async def health_check():
    return {"status": "healthy"}