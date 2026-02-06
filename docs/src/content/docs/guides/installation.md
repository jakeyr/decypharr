---
title: Installation
description: Install Decypharr via Docker or binary.
---

## Docker (Recommended)

### Docker Compose

Create a `docker-compose.yml`:

```yaml
version: '3.8'
services:
  decypharr:
    image: sirrobot01/decypharr:latest
    container_name: decypharr
    ports:
      - "8282:8282"
    volumes:
      - ./config:/config
      - ./downloads:/downloads
      - ./cache:/cache
    restart: unless-stopped
    environment:
      - PUID=1000
      - PGID=1000
      - TZ=America/New_York
```

Run:

```bash
docker compose up -d
```

Access at `http://localhost:8282`

### Docker Run

```bash
docker run -d \
  --name=decypharr \
  -p 8282:8282 \
  -v ./config:/config \
  -v ./downloads:/downloads \
  -v ./cache:/cache \
  -e PUID=1000 \
  -e PGID=1000 \
  sirrobot01/decypharr:latest
```

## Binary

Download the latest release from [GitHub Releases](https://github.com/sirrobot01/decypharr/releases).

```bash
# Extract
tar -xzf decypharr_linux_amd64.tar.gz

# Run
./decypharr
```

Config file will be created at `~/.config/decypharr/config.json`.

## Next Steps

After installation, access the web UI. You'll be redirected to the [Setup Wizard](./quick-start/) for first-run configuration.
