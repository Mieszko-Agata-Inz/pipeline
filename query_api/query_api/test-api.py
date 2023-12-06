from fastapi.testclient import TestClient
from unittest.mock import patch
from query_api.main import app 

client = TestClient(app)

def test_weather_endpoint():
    # Mocking 'sample_weather' and 'actual_weather_async' functions
    with patch('query_api.main.sample_weather', return_value=(["geohash1", "geohash2"], 2)), \
         patch('query_api.main.actual_weather_async') as mock_actual_weather:

        mock_actual_weather.return_value = iter([b'some_weather_data'])  # Mock streaming response

        response = client.get("/weather/usa")  # Example country code
        assert response.status_code == 200
        assert b'some_weather_data' in response.content

def test_stop_endpoint():
    response = client.get("/stop")
    assert response.status_code == 200

    # Optionally, check if the shared data is set to False
    # assert not your_fastapi_app.isOpen
