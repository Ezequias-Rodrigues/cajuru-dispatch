FROM python:3.15-slim

ENV PYTHONUNBUFFERED=1 PATH="/opt/venv/bin:$PATH" UV_PROJECT_ENVIRONMENT=/opt/venv

RUN apt-get update && apt-get install -y --no-install-recommends iproute2 iputils-ping tcpdump procps curl mosquitto-clients && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv && useradd --uid 1000 --create-home dispatch

WORKDIR /opt/cajuru
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

USER dispatch
WORKDIR /workspace
CMD ["fastapi", "run"]
