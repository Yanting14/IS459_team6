FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ingest.py .

RUN useradd --create-home app
USER app

ENTRYPOINT ["python", "ingest.py"]
