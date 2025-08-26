import os
from unittest.mock import patch, MagicMock, ANY
from fastapi.testclient import TestClient
import pytest
from datetime import date, datetime

# Set the TESTING environment variable before importing the app
os.environ["TESTING"] = "true"
from api.main import app, get_db
from api.models import SymbolsMeta, SymbolQuote, StockPrice

client = TestClient(app)

# Fixture to provide a valid API key
@pytest.fixture
def api_key():
    return os.getenv("API_KEY", "test-key")

# Fixture to provide headers with a valid API key
@pytest.fixture
def headers(api_key):
    return {"X-API-Key": api_key}

# List of all task endpoints to be tested
TASK_ENDPOINTS = [
    ("/tasks/symbols/master/run", "api.main.update_symbols_master"),
    ("/tasks/symbols/meta/run", "api.main.update_symbols_meta"),
    ("/tasks/symbols/quote/run", "api.main.update_symbol_quotes"),
    ("/tasks/stocks/price/run", "api.main.update_stock_prices"),
]

# --- Mocks and Fixtures for Data Retrieval ---

# Mock data for a SymbolsMeta record
MOCK_META_DATA = {
    "id": "meta123", "symbol": "TEST", "sector": "Tech", "industry": "Software",
    "marketCap": 1e12, "dividendYield": 0.01, "debtEq": 0.5, "rOE": 0.25,
    "website": "http://test.com", "country": "USA", "description": "A test company.",
    "logo": "http://logo.com/logo.png", "source": "TEST", "lastUpdated": datetime.now()
}

# Mock data for a SymbolQuote record
MOCK_QUOTE_DATA = {
    "id": "quote123", "symbol": "TEST", "price": 100.0, "change": 1.5, "volume": 1000000,
    "pE": 25.0, "pEG": 1.2, "pB": 5.0, "beta": 1.1, "w52High": 120.0, "w52Low": 80.0,
    "recommendation": "buy", "lastUpdated": datetime.now()
}

# Mock data for a StockPrice record
MOCK_PRICE_DATA = {
    "symbol": "TEST", "date": date(2023, 1, 1), "open": 98.0, "high": 102.0,
    "low": 97.5, "close": 100.0, "volume": 1000000
}

@pytest.fixture
def mock_db_cursor():
    """Fixture to mock the database cursor."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    # This context manager simulates `with conn.cursor() as cursor:`
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Override the get_db dependency to return our mock connection
    app.dependency_overrides[get_db] = lambda: mock_conn
    yield mock_cursor
    # Clean up the override after the test
    app.dependency_overrides = {}


# --- Tests for Task Endpoints ---

# Use pytest.mark.parametrize to run the same test logic for multiple endpoints
@pytest.mark.parametrize("endpoint, task_path", TASK_ENDPOINTS)
def test_task_endpoint_unauthorized(endpoint, task_path):
    """Tests that task endpoints return 403 if the API key is wrong."""
    response = client.post(endpoint, headers={"X-API-Key": "wrong-key"})
    assert response.status_code == 403

@pytest.mark.parametrize("endpoint, task_path", TASK_ENDPOINTS)
def test_task_endpoint_success(headers, endpoint, task_path):
    """Tests that task endpoints can be triggered successfully."""
    # Patch the actual task function to avoid running it
    with patch(task_path) as mock_task:
        response = client.post(endpoint, headers=headers)
        assert response.status_code == 200
        assert "has been triggered" in response.json()["message"]
        # Assert that the background task function was called
        mock_task.assert_called_once()


# --- Tests for Data Retrieval Endpoints ---

def test_get_symbol_meta_success(headers, mock_db_cursor):
    """Test successful retrieval of symbol metadata."""
    mock_db_cursor.fetchone.return_value = tuple(MOCK_META_DATA.values())
    mock_db_cursor.description = [(key,) for key in MOCK_META_DATA.keys()]

    response = client.get("/symbols/meta/TEST", headers=headers)

    assert response.status_code == 200
    # Pydantic will convert datetime to ISO string in the JSON response
    assert response.json()["symbol"] == "TEST"
    assert response.json()["sector"] == "Tech"

def test_get_symbol_meta_not_found(headers, mock_db_cursor):
    """Test 404 for non-existent symbol metadata."""
    mock_db_cursor.fetchone.return_value = None

    response = client.get("/symbols/meta/NONEXISTENT", headers=headers)

    assert response.status_code == 404

def test_get_symbol_quote_success(headers, mock_db_cursor):
    """Test successful retrieval of a symbol quote."""
    mock_db_cursor.fetchone.return_value = tuple(MOCK_QUOTE_DATA.values())
    mock_db_cursor.description = [(key,) for key in MOCK_QUOTE_DATA.keys()]

    response = client.get("/symbols/quote/TEST", headers=headers)

    assert response.status_code == 200
    assert response.json()["symbol"] == "TEST"
    assert response.json()["price"] == 100.0

def test_get_stock_price_history_success(headers, mock_db_cursor):
    """Test successful retrieval of stock price history."""
    mock_db_cursor.fetchall.return_value = [tuple(MOCK_PRICE_DATA.values())]
    mock_db_cursor.description = [(key,) for key in MOCK_PRICE_DATA.keys()]

    response = client.get("/symbols/stocks/TEST/price", headers=headers)

    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert response.json()[0]["symbol"] == "TEST"
    assert response.json()[0]["close"] == 100.0

def test_get_stock_price_history_with_dates(headers, mock_db_cursor):
    """Test retrieval of stock price history with date filters."""
    mock_db_cursor.fetchall.return_value = [tuple(MOCK_PRICE_DATA.values())]
    mock_db_cursor.description = [(key,) for key in MOCK_PRICE_DATA.keys()]

    start_date = "2023-01-01"
    end_date = "2023-01-31"

    response = client.get(
        f"/symbols/stocks/TEST/price?start_date={start_date}&end_date={end_date}",
        headers=headers
    )

    assert response.status_code == 200
    # Check that the SQL query was called with the correct date parameters
    call_args = mock_db_cursor.execute.call_args[0]
    sql_query = call_args[0]
    sql_params = call_args[1]

    assert "date >= %s" in sql_query
    assert "date <= %s" in sql_query
    assert date(2023, 1, 1) in sql_params
    assert date(2023, 1, 31) in sql_params


# --- Tests for External Data Endpoint ---

MOCK_UPDATED_DATA = {
    "meta": MOCK_META_DATA,
    "quote": MOCK_QUOTE_DATA,
    "price_history": [MOCK_PRICE_DATA],
}

@patch("api.main.update_symbol_from_external_source")
def test_update_symbol_from_source_success(mock_update, headers):
    """Test successful update of a symbol from an external source."""
    # Configure the async mock to return a value
    mock_update.return_value = MOCK_UPDATED_DATA

    response = client.post("/symbols/external/TEST", headers=headers)

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["meta"]["symbol"] == "TEST"
    assert json_response["quote"]["price"] == 100.0
    assert len(json_response["price_history"]) == 1
    # Check that the underlying task was called correctly
    mock_update.assert_called_once()
    # To check arguments of an async function, you can inspect call_args
    args, kwargs = mock_update.call_args
    assert args[0] == "TEST"  # symbol
    assert len(args) == 2  # symbol and conn
    # A simple way to check if it's a connection-like object
    assert hasattr(args[1], "cursor")


@patch("api.main.update_symbol_from_external_source")
def test_update_symbol_from_source_not_found(mock_update, headers):
    """Test 404 when updating a symbol that cannot be found."""
    mock_update.return_value = {"meta": None, "quote": None, "price_history": []}

    response = client.post("/symbols/external/NONEXISTENT", headers=headers)

    assert response.status_code == 404
