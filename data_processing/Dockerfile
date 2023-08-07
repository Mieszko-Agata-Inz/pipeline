FROM python:3.9

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./data_processing /code/data_processing

CMD ["python", "-m", "bytewax.run", "/code/data_processing/processing:flow"]  