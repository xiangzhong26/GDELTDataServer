FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml /app/
COPY gdelt_server /app/gdelt_server
RUN pip install --no-cache-dir . && useradd --uid 10001 --create-home gdelt && mkdir /data && chown gdelt:gdelt /data
USER gdelt
EXPOSE 8800
CMD ["python", "-m", "gdelt_server", "--config", "/app/config.local.json", "serve"]
