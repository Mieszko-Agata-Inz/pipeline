FROM python:3.9

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./redis_caching /code/redis_caching

CMD ["python", "/code/redis_caching/redis_caching.py"]  