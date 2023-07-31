FROM apache/spark-py:v3.4.0

# RUN add-apt-repository ppa:webupd8team/java && \
#     apt-get update && \
#     apt-get install -y openjdk-8-jdk && \
#     apt-get install -y ant && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/ && \
#     rm -rf /var/cache/oracle-jdk8-installer;
    
# ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
# RUN export JAVA_HOME
USER 0

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./processing.py /code/processing.py

EXPOSE 8080

CMD ["python3", "processing.py"]
