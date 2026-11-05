FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY scoring/requirements.txt scoring/requirements-cloud.txt /tmp/
# CPU inference is sufficient for the measured daily workload. A GPU build can
# override this URL to https://download.pytorch.org/whl/cu121.
ARG TORCH_INDEX_URL=https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir torch==2.5.1 --index-url ${TORCH_INDEX_URL} \
    && pip install --no-cache-dir -r /tmp/requirements-cloud.txt
COPY scoring /app/scoring
RUN mkdir /scratch
ENV SCORING_WORK_DIR=/scratch
ENTRYPOINT ["python", "-m", "scoring.cloud"]
