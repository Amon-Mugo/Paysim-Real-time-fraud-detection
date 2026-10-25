#used to containerize the kafaka generator script 

FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements-generator.txt .
RUN pip install --no-cache-dir -r requirements-generator.txt

COPY generator/ generator/
RUN useradd --create-home --uid 10001 appuser \
    && mkdir /app/data \
    && chown appuser:appuser /app/data

USER appuser 
ENTRYPOINT ["python", "-m", "generator.main"]