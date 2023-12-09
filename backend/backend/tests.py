from fastapi.testclient import TestClient
from main import app
import pytest
import io

client = TestClient(app)

def test_successful_update_cold_model():
    # Mock file upload
    data = {
        'req_api_key': '12345',
        'file': ('filename', io.BytesIO(b'test data'), 'text/plain')
    }
    with 
    response = client.post("/update/cold/xgb1", files=data)
    assert response.status_code == 200
    assert response.json() == 1  # or whatever the successful response is
# def test_update_failure_wrong_api_key():
#     # Test with incorrect API key

# def test_update_failure_invalid_state():
#     # Test with invalid state
