FROM python:3.9

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./query_api /code/query_api
COPY ./simulate_request.py /code/simulate_request.py

EXPOSE 8080

# CMD ["uvicorn", "query_api.main:app", "--host", "0.0.0.0", "--port", "80", "&&", "python", "query_api.simulate_request.py" uvicorn query_api.main:app --host 0.0.0.0 --port 80 && ]
CMD ["bash", "-c", "uvicorn query_api.main:app --host 0.0.0.0 --port 80 & python3 simulate_request.py"]