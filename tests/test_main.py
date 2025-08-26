from fastapi.testclient import TestClient
import os
os.environ["TESTING"] = "true"
from api.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}