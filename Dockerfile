FROM python:3.11-slim

# Deno is yt-dlp's supported JavaScript runtime for solving the YouTube "n"
# challenge (EJS). Debian's nodejs (v20) is reported "unsupported" by yt-dlp,
# so install Deno instead. Paired with the yt-dlp-ejs package (requirements).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://github.com/denoland/deno/releases/latest/download/deno-x86_64-unknown-linux-gnu.zip -o /tmp/deno.zip \
    && unzip -q /tmp/deno.zip -d /usr/local/bin \
    && rm /tmp/deno.zip \
    && deno --version
# Deno needs a writable cache dir at runtime
ENV DENO_DIR=/tmp/deno_cache

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "stream_monitor.py"]
