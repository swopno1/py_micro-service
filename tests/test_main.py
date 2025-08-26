from fastapi.testclient import TestClient
import os
os.environ["TESTING"] = "true"
from datetime import date, datetime
from unittest.mock import MagicMock, patch
from api.main import app, get_db, fetch_and_store_eod_data
from api.fetcher import fetch_stock_data

client = TestClient(app)

def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_get_latest_stock_price_unauthorized():
    response = client.get("/stock/AAPL", headers={"X-API-Key": "wrong-key"})
    assert response.status_code == 403
    assert response.json() == {"detail": "Could not validate credentials."}

def test_get_latest_stock_price_not_found():
    # This test assumes the symbol 'NOSYMBOL' does not exist in the DB.
    api_key = os.getenv("API_KEY")
    headers = {"X-API-Key": api_key}

    response = client.get("/stock/NOSYMBOL", headers=headers)
    assert response.status_code == 404
    assert response.json() == {"detail": "No stock price data found for symbol NOSYMBOL"}

def test_get_latest_stock_price_success():
    # 1. Define the mock data that the database should return
    mock_record = {
        "symbol": "AAPL",
        "date": date(2023, 10, 26),
        "open": 170.0,
        "high": 172.5,
        "low": 169.5,
        "close": 172.0,
        "volume": 98765432,
    }

    conn = next(get_db())
    try:
        with conn.cursor() as cursor:
            # Clean up any previous test data
            cursor.execute('DELETE FROM "StockPrice" WHERE symbol = %s', ("AAPL",))
            cursor.execute('DELETE FROM "SymbolsMaster" WHERE symbol = %s', ("AAPL",))
            # Insert a test record
            cursor.execute(
                """
                INSERT INTO "SymbolsMaster" (id, symbol, name, source, status, "lastUpdated")
                VALUES (%s, %s, %s, 'YAHOO', 'active', NOW())
                """,
                ("test-master-id", "AAPL", "Apple Inc."),
            )
            cursor.execute(
                """
                INSERT INTO "StockPrice" (id, symbol, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                ("test-price-id", "AAPL", date(2023, 10, 26), 170.0, 172.5, 169.5, 172.0, 98765432),
            )
            conn.commit()

        # 4. Make the request with the correct API key
        api_key = os.getenv("API_KEY")
        response = client.get("/stock/AAPL", headers={"X-API-Key": api_key})

        # 5. Assert the results
        assert response.status_code == 200
        assert response.json()["symbol"] == "AAPL"
        assert response.json()["close"] == 172.0
    finally:
        # 6. Clean up the test data
        conn = next(get_db())
        with conn.cursor() as cursor:
            cursor.execute('DELETE FROM "StockPrice" WHERE symbol = %s', ("AAPL",))
            conn.commit()
        conn.close()


@patch("yfinance.Ticker")
def test_fetch_and_store_eod_data(MockTicker):
    # 1. Setup mock for yfinance.Ticker
    mock_ticker_instance = MockTicker.return_value
    mock_ticker_instance.info = {
        "longName": "Apple Inc.",
        "exchange": "NMS",
        "quoteType": "EQUITY",
        "currency": "USD",
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "marketCap": 2.7e12,
        "dividendYield": 0.005,
        "debtToEquity": 1.2,
        "returnOnEquity": 0.5,
        "website": "https://www.apple.com",
        "country": "United States",
        "longBusinessSummary": "An American multinational technology company.",
        "logo_url": "https://logo.clearbit.com/apple.com",
        "regularMarketPrice": 172.0,
        "regularMarketChange": 1.0,
        "regularMarketVolume": 98765432,
        "trailingPE": 28.0,
        "pegRatio": 1.5,
        "priceToBook": 40.0,
        "beta": 1.2,
        "fiftyTwoWeekHigh": 180.0,
        "fiftyTwoWeekLow": 120.0,
        "recommendationKey": "buy",
    }
    # Create a mock for the history method
    mock_hist = MagicMock()
    mock_hist.empty = False
    mock_hist.iloc.__getitem__.return_value = {
        "Open": 170.0,
        "High": 172.5,
        "Low": 169.5,
        "Close": 172.0,
        "Volume": 98765432,
    }
    mock_hist.index = [datetime(2023, 1, 1)]
    mock_hist.to_dict.return_value = {
        "Open": {0: 170.0},
        "High": {0: 172.5},
        "Low": {0: 169.5},
        "Close": {0: 172.0},
        "Volume": {0: 98765432},
    }
    mock_ticker_instance.history.return_value = mock_hist


    # 2. Setup the database
    conn = next(get_db())
    try:
        with conn.cursor() as cursor:
            # Clean up any previous test data
            cursor.execute('DELETE FROM "StockPrice"')
            cursor.execute('DELETE FROM "SymbolQuote"')
            cursor.execute('DELETE FROM "SymbolsMeta"')
            cursor.execute('DELETE FROM "SymbolsMaster"')
            # Insert a test symbol
            cursor.execute(
                """
                INSERT INTO "SymbolsMaster" (id, symbol, name, source, status, "lastUpdated")
                VALUES (%s, %s, %s, 'YAHOO', 'active', NOW())
                """,
                ("test-id", "TESTTICKER", "Test Ticker"),
            )
            conn.commit()

        # 3. Call the function
        with patch("api.main.fetch_stock_data") as mock_fetch:
             mock_fetch.return_value = fetch_stock_data("TESTTICKER")
             fetch_and_store_eod_data()


        # 4. Assert that the data was inserted correctly
        with conn.cursor() as cursor:
            cursor.execute('SELECT * FROM "SymbolsMeta" WHERE symbol = %s', ("TESTTICKER",))
            meta_record = cursor.fetchone()
            assert meta_record is not None
            assert meta_record["sector"] == "Technology"

            cursor.execute('SELECT * FROM "SymbolQuote" WHERE symbol = %s', ("TESTTICKER",))
            quote_record = cursor.fetchone()
            assert quote_record is not None
            assert quote_record["price"] == 172.0

            cursor.execute('SELECT * FROM "StockPrice" WHERE symbol = %s', ("TESTTICKER",))
            price_record = cursor.fetchone()
            assert price_record is not None
            assert price_record["close"] == 172.0

    finally:
        # 5. Clean up the test data
        conn = next(get_db())
        with conn.cursor() as cursor:
            cursor.execute('DELETE FROM "StockPrice" WHERE symbol = %s', ("TESTTICKER",))
            cursor.execute('DELETE FROM "SymbolQuote" WHERE symbol = %s', ("TESTTICKER",))
            cursor.execute('DELETE FROM "SymbolsMeta" WHERE symbol = %s', ("TESTTICKER",))
            cursor.execute('DELETE FROM "SymbolsMaster" WHERE symbol = %s', ("TESTTICKER",))
            conn.commit()
        conn.close()