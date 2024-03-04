FROM docker.io/bitnami/spark:3.5

#you may need to update this in case the docker-compose version changes

ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
ENV PATH="${HOME}/.local/bin/:$PATH"

USER root
# Upgrade packages to the latest version
RUN apt-get -y update && \
    apt-get install wget gcc python3-dev  -qqq && \
    python3 -m pip install --upgrade pip

# the rootless user ID
USER 1001

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /code
