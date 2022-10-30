ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.10

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

COPY --from=py3 / /

ARG PYSPARK_VERSION=3.2.0
RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}

WORKDIR /app

RUN pip install --upgrade pip pipenv wheel setuptools

COPY ./Pipfile /app/
COPY ./Pipfile.lock /app/

RUN pipenv install

COPY . /app

CMD ["pipenv", "run", "uvicorn", "--reload", "main:app", "--host", "0.0.0.0", "--port", "6000"]
