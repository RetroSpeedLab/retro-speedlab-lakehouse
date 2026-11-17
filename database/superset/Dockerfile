FROM apache/superset:6.1.0@sha256:16b50bbef6648912a79e3293d418fefa743ce555d36c5af6b85d859119ed7f88
USER root
COPY requirements.txt /tmp/requirements.txt
RUN uv pip install --python /app/.venv/bin/python --no-deps --require-hashes --requirement /tmp/requirements.txt
COPY superset_config.py /app/pythonpath/superset_config.py
COPY assets.py bundle.py charts.py dashboards.py datasets.py dataset_names.py lake_datasets.py owner.py sync_datasets.py start.sh /app/
USER superset
ENV SERVER_WORKER_AMOUNT=1 SERVER_THREADS_AMOUNT=8 GUNICORN_TIMEOUT=300
CMD ["bash", "/app/start.sh"]
