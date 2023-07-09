FROM python:3.9

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./query_api /code/query_api

EXPOSE 8080

CMD ["uvicorn", "query_api.main:app", "--host", "0.0.0.0", "--port", "80"]

