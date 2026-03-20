FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    ca-certificates \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# yt-dlp's YouTube extractor increasingly relies on running small JS snippets (EJS).
# Install a standalone deno binary (avoid flaky install.sh and keep layers cacheable).
RUN set -eux; \
    arch="$(uname -m)"; \
    case "${arch}" in \
      aarch64|arm64) asset="deno-aarch64-unknown-linux-gnu.zip" ;; \
      x86_64|amd64) asset="deno-x86_64-unknown-linux-gnu.zip" ;; \
      *) echo "unsupported arch: ${arch}" >&2; exit 2 ;; \
    esac; \
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 2 \
      "https://github.com/denoland/deno/releases/latest/download/${asset}" \
      -o /tmp/deno.zip; \
    unzip /tmp/deno.zip -d /usr/local/bin; \
    rm -f /tmp/deno.zip; \
    chmod +x /usr/local/bin/deno

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
