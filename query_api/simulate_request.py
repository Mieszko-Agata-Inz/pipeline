from fastapi.testclient import TestClient
from time import sleep
from query_api.main import app


# Function to simulate a request to /weather/{country} after startup
def simulate_request_after_startup():
    # Wait for FastAPI to fully start
    sleep(5)

    # Use TestClient to simulate a request
    client = TestClient(app)
    response = client.get("/weather/Poland")
    print(response.status_code)


if __name__ == "__main__":
    # Run the simulation after the application starts
    simulate_request_after_startup()
