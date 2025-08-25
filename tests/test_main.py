from fastapi.testclient import TestClient
import os
from datetime import date
from unittest.mock import MagicMock
from api.main import app, get_db

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

    # 2. Create a mock database connection and cursor
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = mock_record

    mock_conn = MagicMock()
    # This setup mocks the `with conn.cursor() as cursor:` context manager
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # 3. Define and apply the dependency override for get_db
    app.dependency_overrides[get_db] = lambda: mock_conn

    # 4. Make the request with the correct API key
    api_key = os.getenv("API_KEY")
    response = client.get("/stock/AAPL", headers={"X-API-Key": api_key})

    # 5. Assert the results
    assert response.status_code == 200
    assert response.json()["symbol"] == "AAPL"
    assert response.json()["close"] == 172.0

    # 6. Clean up the override to not affect other tests
    app.dependency_overrides.clear()