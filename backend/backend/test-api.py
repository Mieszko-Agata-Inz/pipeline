import json
from unittest.mock import Mock, patch
from backend.utils.raw_data import get_raw_data
from backend.utils.forecasts import get_forecast
from backend.main import update_model

import pytest
from fastapi import UploadFile
import io
import datetime


# Mock data for testing
api_key = "12345"  # Replace with the actual API key used in your function
incorrect_api_key = "wrong_api_key"
model_name_cold = "xgb_1"  # Replace with a model name that exists in coldstart_models
model_name_hot = "lstm_1"  # Replace with a model name that exists in hot_models
model_name_invalid = "non_existing_model"
file_content = b"mock file content"
file = UploadFile(file=io.BytesIO(file_content), filename="model.pkl")

# Mock the coldstart_models and hot_models dictionaries
coldstart_models = {model_name_cold: ("model.pkl", None)}  # Mock structure
hot_models = {model_name_hot: ("model.pkl", None)}  # Mock structure


@pytest.mark.asyncio
async def test_api_key_authentication():
    result = await update_model("cold", model_name_cold, incorrect_api_key, file)
    assert result is False, "API key authentication failed"


@pytest.mark.asyncio
async def test_cold_model_update():
    result = await update_model("cold", model_name_cold, api_key, file)
    assert result != 1, "Cold model update failed"


@pytest.mark.asyncio
async def test_invalid_state_handling():
    result = await update_model("invalid_state", model_name_invalid, api_key, file)
    assert result == "wrong model list", "Invalid state handling failed"


# Fixture for mocking Redis client
@pytest.fixture
def mock_redis():
    with patch("backend.utils.forecasts.redisCli") as mock:
        yield mock


# Fixture for mocking models
@pytest.fixture
def mock_models():
    with patch(
        "backend.main.coldstart_models",
        new_callable=lambda: {
            "xgb_1": (None, Mock()),
            "xgb_2": (None, Mock()),
            "xgb_3": (None, Mock()),
        },
    ), patch(
        "backend.main.hot_models",
        new_callable=lambda: {
            "lstm_1": (None, Mock()),
            "lstm_2": (None, Mock()),
            "lstm_3": (None, Mock()),
        },
    ):
        yield


def test_no_data(mock_redis):
    # Mocking Redis to return no documents
    mock_redis.ft().search.return_value = Mock(docs=[])

    # Calling the function with mocked Redis
    result = get_forecast("some_lat", "some_lon", {}, {}, {}, {})

    # Asserting that the function returns 'no data'
    assert result == "no data"


class dummy_xgb:
    def predict(*args):
        return 0


def test_cold_model_prediction(mock_redis):
    # Mocking Redis to return one document for cold start
    mock_redis.ft().search.side_effect = [
        Mock(docs=[]),
        Mock(
            docs=[
                Mock(
                    json=json.dumps(
                        {
                            "humidity": 50,
                            "wind_v": 10,
                            "temp": 20,
                            "timestamp": datetime.datetime.timestamp(
                                datetime.datetime.utcnow()
                            ),
                        }
                    )
                )
            ]
        ),
    ]
    # Calling the function with mocked dependencies
    result = get_forecast(
        "some_lat",
        "some_lon",
        {
            "xgb_1": [-1, dummy_xgb()],
            "xgb_2": [-1, dummy_xgb()],
            "xgb_3": [-1, dummy_xgb()],
        },
        {},
        {},
        {
            "xgb_1": [-1, {"humid": 1, "wind": 1, "temp": 1}],
            "xgb_2": [-1, {"humid": 2, "wind": 2, "temp": 2}],
            "xgb_3": [-1, {"humid": 3, "wind": 3, "temp": 3}],
        },
    )

    # Asserting that the function returns expected model prediction
    assert result == {
        "hour0": [50, 10, 20],
        "hour1": [1, 1, 1],
        "hour2": [2, 2, 2],
        "hour3": [3, 3, 3],
    }


@pytest.fixture
def mock_redis2():
    with patch("backend.utils.raw_data.redisCli") as mock:
        mock.ft().search.return_value = Mock(docs=["mocked_data"])
        yield mock


def test_get_raw_data(mock_redis2):
    # Mock current time
    current_time = datetime.datetime(2023, 1, 1, 12, 0, 0)
    timestamp = (
        datetime.datetime.timestamp(current_time) - 30
    )  # 30 seconds before current time

    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = current_time

        # Call the function
        result = get_raw_data(timestamp)

        # Assertions to ensure correct Redis call and return value
        mock_redis2.ft().search.assert_called_once()
        assert result.docs == ["mocked_data"]
