FROM python:3.12-slim@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704
ENV PYTHONUNBUFFERED=1 UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv VIRTUAL_ENV=/opt/venv PATH="/opt/venv/bin:/opt/java/bin:${PATH}" \
    JAVA_HOME=/opt/java PYSPARK_PYTHON=/opt/venv/bin/python AIRFLOW_HOME=/lake/airflow
RUN apt-get update \
    && apt-get install --yes --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /bin/
ARG WHEELHOUSE=http://127.0.0.1:8765
COPY jars.lock java.lock /tmp/
RUN read -r java_sum java_file </tmp/java.lock     && python -c "import sys, urllib.request; urllib.request.urlretrieve(sys.argv[1], '/tmp/java.tgz')"         "${WHEELHOUSE}/java/${java_file}"     && echo "${java_sum}  /tmp/java.tgz" | sha256sum --check     && mkdir /opt/java && tar -xzf /tmp/java.tgz -C /opt/java --strip-components=1 && rm /tmp/java.tgz     && mkdir /opt/spark-jars     && while read -r sum group artifact version; do         jar="${artifact}-${version}.jar";         python -c "import sys, urllib.request; urllib.request.urlretrieve(sys.argv[1], sys.argv[2])"             "${WHEELHOUSE}/spark-jars/${jar}" "/opt/spark-jars/${jar}"         && echo "${sum}  /opt/spark-jars/${jar}" | sha256sum --check || exit 1;     done </tmp/jars.lock
WORKDIR /lakehouse
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-dev --no-emit-project --quiet --output-file /tmp/requirements.txt \
    && uv venv /opt/venv \
    && uv pip install --no-index --find-links "${WHEELHOUSE}/" --requirement /tmp/requirements.txt
COPY . .
RUN uv pip install --no-deps --no-index --find-links "${WHEELHOUSE}/" .
