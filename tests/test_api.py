import os
from unittest.mock import patch, MagicMock, ANY
from fastapi.testclient import TestClient
import pytest
from datetime import date, datetime

# Set environment variables for testing
os.environ["TESTING"] = "true"
os.environ["API_KEY"] = "test-key"
os.environ["DATABASE_URL"] = "postgresql://test:test@localhost:5432/testdb"
from api.main import app, get_db
from api.models import SymbolsMeta, SymbolQuote, StockPrice

from api.models import SymbolsMeta, SymbolQuote, StockPrice

# Fixture to provide a valid API key
@pytest.fixture
def api_key():
    return os.getenv("API_KEY")

# Fixture to provide headers with a valid API key
@pytest.fixture
def headers(api_key):
    return {"X-API-Key": api_key}

@pytest.fixture
def client(monkeypatch):
    """Fixture to create a test client with mocked dependencies."""
    # Mock the fetcher manager before the app is imported
    mock_fetcher = MagicMock()
    monkeypatch.setattr("api.fetcher.fetcher_manager", mock_fetcher)

    # Now import the app
    from api.main import app, get_db

    # Mock the database connection
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    def get_mock_db():
        yield mock_conn

    app.dependency_overrides[get_db] = get_mock_db

    test_client = TestClient(app)
    # Attach the mock cursor to the client for easy access in tests
    test_client.mock_cursor = mock_cursor
    yield test_client

    # Clean up dependency overrides
    app.dependency_overrides = {}


# List of all task endpoints to be tested
TASK_ENDPOINTS = [
    ("/tasks/symbols/master/run", "api.main.update_symbols_master"),
    ("/tasks/symbols/meta/run", "api.main.update_symbols_meta"),
    ("/tasks/symbols/quote/run", "api.main.update_symbol_quotes"),
    ("/tasks/stocks/price/run", "api.main.update_stock_prices"),
]

# --- Mocks for Data Retrieval ---

MOCK_META_DATA = {
    "id": "meta123", "symbol": "TEST", "sector": "Tech", "industry": "Software",
    "marketCap": 1e12, "dividendYield": 0.01, "debtEq": 0.5, "rOE": 0.25,
    "website": "http://test.com", "country": "USA", "description": "A test company.",
    "logo": "http://logo.com/logo.png", "source": "TEST", "lastUpdated": datetime.now()
}

MOCK_QUOTE_DATA = {
    "id": "quote123", "symbol": "TEST", "price": 100.0, "change": 1.5, "volume": 1000000,
    "pE": 25.0, "pEG": 1.2, "pB": 5.0, "beta": 1.1, "w52High": 120.0, "w52Low": 80.0,
    "recommendation": "buy", "lastUpdated": datetime.now()
}

MOCK_PRICE_DATA = {
    "symbol": "TEST", "date": date(2023, 1, 1), "open": 98.0, "high": 102.0,
    "low": 97.5, "close": 100.0, "volume": 1000000
}


# --- Tests for Task Endpoints ---

@pytest.mark.parametrize("endpoint, task_path", TASK_ENDPOINTS)
def test_task_endpoint_unauthorized(client, endpoint, task_path):
    """Tests that task endpoints return 403 if the API key is wrong."""
    response = client.post(endpoint, headers={"X-API-Key": "wrong-key"})
    assert response.status_code == 403

@pytest.mark.parametrize("endpoint, task_path", TASK_ENDPOINTS)
def test_task_endpoint_success(client, headers, endpoint, task_path):
    """Tests that task endpoints can be triggered successfully."""
    with patch(task_path) as mock_task:
        response = client.post(endpoint, headers=headers)
        assert response.status_code == 200
        assert "has been triggered" in response.json()["message"]
        mock_task.assert_called_once()


# --- Tests for Data Retrieval Endpoints ---

def test_get_symbol_meta_success(client, headers):
    """Test successful retrieval of symbol metadata."""
    client.mock_cursor.fetchone.return_value = tuple(MOCK_META_DATA.values())
    client.mock_cursor.description = [(key,) for key in MOCK_META_DATA.keys()]

    response = client.get("/symbols/meta/TEST", headers=headers)

    assert response.status_code == 200
    assert response.json()["symbol"] == "TEST"
    assert response.json()["sector"] == "Tech"

def test_get_symbol_meta_not_found(client, headers):
    """Test 404 for non-existent symbol metadata."""
    client.mock_cursor.fetchone.return_value = None

    response = client.get("/symbols/meta/NONEXISTENT", headers=headers)

    assert response.status_code == 404

def test_get_symbol_quote_success(client, headers):
    """Test successful retrieval of a symbol quote."""
    client.mock_cursor.fetchone.return_value = tuple(MOCK_QUOTE_DATA.values())
    client.mock_cursor.description = [(key,) for key in MOCK_QUOTE_DATA.keys()]

    response = client.get("/symbols/quote/TEST", headers=headers)

    assert response.status_code == 200
    assert response.json()["symbol"] == "TEST"
    assert response.json()["price"] == 100.0

def test_get_stock_price_history_success(client, headers):
    """Test successful retrieval of stock price history."""
    client.mock_cursor.fetchall.return_value = [tuple(MOCK_PRICE_DATA.values())]
    client.mock_cursor.description = [(key,) for key in MOCK_PRICE_DATA.keys()]

    response = client.get("/symbols/stocks/TEST/price", headers=headers)

    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert response.json()[0]["symbol"] == "TEST"

def test_get_stock_price_history_with_dates(client, headers):
    """Test retrieval of stock price history with date filters."""
    client.mock_cursor.fetchall.return_value = [tuple(MOCK_PRICE_DATA.values())]
    client.mock_cursor.description = [(key,) for key in MOCK_PRICE_DATA.keys()]

    start_date = "2023-01-01"
    end_date = "2023-01-31"

    response = client.get(
        f"/symbols/stocks/TEST/price?start_date={start_date}&end_date={end_date}",
        headers=headers
    )

    assert response.status_code == 200
    call_args = client.mock_cursor.execute.call_args[0]
    sql_query = call_args[0]
    sql_params = call_args[1]

    assert "date >= %s" in sql_query
    assert "date <= %s" in sql_query
    assert date(2023, 1, 1) in sql_params
    assert date(2023, 1, 31) in sql_params


# --- Tests for External Data Endpoint ---

MOCK_UPDATED_DATA = {
    "meta": SymbolsMeta(**MOCK_META_DATA),
    "quote": SymbolQuote(**MOCK_QUOTE_DATA),
    "price_history": [StockPrice(**MOCK_PRICE_DATA)],
}


@patch("api.main.update_symbol_from_external_source", new_callable=MagicMock)
def test_update_symbol_from_source_success(mock_update, client, headers):
    """Test successful update of a symbol from an external source."""
    # Since the real function is async, the mock should be an async-compatible mock
    async def async_mock_update(*args, **kwargs):
        return MOCK_UPDATED_DATA

    mock_update.side_effect = async_mock_update

    response = client.post("/symbols/external/TEST", headers=headers)

    assert response.status_code == 200
    json_response = response.json()
    assert json_response["meta"]["symbol"] == "TEST"
    assert json_response["quote"]["price"] == 100.0
    mock_update.assert_called_once()
    args, kwargs = mock_update.call_args
    assert args[0] == "TEST"
    assert hasattr(args[1], "cursor")


@patch("api.main.update_symbol_from_external_source")

def test_update_symbol_from_source_not_found(mock_update, client, headers):

    """Test 404 when updating a symbol that cannot be found."""
    async def async_mock_update(*args, **kwargs):
        return {"meta": None, "quote": None, "price_history": []}

    mock_update.side_effect = async_mock_update

    response = client.post("/symbols/external/NONEXISTENT", headers=headers)

    assert response.status_code == 404
