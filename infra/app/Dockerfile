FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md LICENSE /app/
COPY docs /app/docs
COPY src /app/src
COPY frontend /app/frontend

RUN python -m pip install --upgrade pip \
    && python -m pip install -e .

RUN mkdir -p /app/data /app/runtime

EXPOSE 8000

CMD ["saw", "serve", "--strict-runtime-checks"]
