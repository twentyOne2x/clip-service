FROM python:3.11.15-slim-bookworm@sha256:d29f48a31a8b408ed19272ca1e7b10ebae13b240a27e862d3d4217c528e2e0c3

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    ca-certificates \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# yt-dlp's YouTube extractor increasingly relies on running small JS snippets (EJS).
# Pin both the immutable Deno release and the official per-architecture asset digest.
ARG DENO_VERSION=2.9.5
RUN set -eux; \
    arch="$(uname -m)"; \
    case "${arch}" in \
      aarch64|arm64) \
        asset="deno-aarch64-unknown-linux-gnu.zip"; \
        asset_sha256="6b7cae3a8fc4385a59dea3146fcb8bad7fea4230e0ad36a8c692afacbc254be0" \
        ;; \
      x86_64|amd64) \
        asset="deno-x86_64-unknown-linux-gnu.zip"; \
        asset_sha256="8b010a3b1a4a0188a67cdb8a7a27348b2a501af78aec7fc74f2ace167368d530" \
        ;; \
      *) echo "unsupported arch: ${arch}" >&2; exit 2 ;; \
    esac; \
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 2 \
      "https://github.com/denoland/deno/releases/download/v${DENO_VERSION}/${asset}" \
      -o /tmp/deno.zip; \
    echo "${asset_sha256}  /tmp/deno.zip" | sha256sum -c -; \
    unzip /tmp/deno.zip -d /usr/local/bin; \
    rm -f /tmp/deno.zip; \
    chmod 0755 /usr/local/bin/deno; \
    deno --version

RUN groupadd --system --gid 10001 clip-service \
    && useradd --system --uid 10001 --gid 10001 --create-home --home-dir /home/clip-service clip-service \
    && install -d -o 10001 -g 10001 -m 0750 \
      /app \
      /var/lib/icmfyi/clips \
      /home/clip-service/.cache/yt-dlp

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chown root:root /app \
    && chmod 0755 /app \
    && find /app -xdev -type d -exec chmod go-w {} + \
    && find /app -xdev -type f -exec chmod go-w {} + \
    && python -m py_compile main.py persistence.py

ENV CLIP_OUTPUT_DIRECTORY=/var/lib/icmfyi/clips \
    HOME=/home/clip-service

USER 10001:10001

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
