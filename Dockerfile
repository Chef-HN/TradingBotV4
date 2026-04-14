FROM python:3.12-slim

WORKDIR /app

# supervisor manages the two processes (worker + api) inside one container
RUN apt-get update && apt-get install -y --no-install-recommends \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies (layer cached until pyproject.toml or src/ changes)
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e .

COPY migrations/ migrations/
COPY supervisord.conf /etc/supervisor/conf.d/tradingbot.conf
COPY docker-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Needed so `python -m scripts.*` resolves packages from src/
ENV PYTHONPATH=/app/src

EXPOSE 8090

ENTRYPOINT ["/entrypoint.sh"]
