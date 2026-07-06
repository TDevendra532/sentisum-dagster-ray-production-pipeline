# One image for the Dagster app and any Ray head/workers you add — so your code is
# importable everywhere. Ray 2.55.1 + Dagster 1.13.7 on Python 3.11.
FROM rayproject/ray:2.55.1-py311

RUN pip install --no-cache-dir dagster==1.13.7 dagster-webserver==1.13.7 pyyaml

WORKDIR /app
ENV PYTHONPATH=/app
# Source is bind-mounted at /app by docker-compose, so edits need no rebuild.
