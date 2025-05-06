# docker_pull.py

Python script to pull lots of docker images in parallel

https://github.com/user-attachments/assets/98832e30-0a05-4789-b055-a825cbba1ba5

## Dependencies

Python: `requests`, `urllib3`

## Usage

`./docker_pull.py <full image tag> <full image tag>`

`./docker_pull.py docker.io/nginx:latest docker.io/ubuntu:latest`

```sh
usage: docker_pull.py [-h] [-x THROTTLE] [-q] [--debug] [--verbose] [images ...]

positional arguments:
  images

options:
  -h, --help            show this help message and exit
  -x THROTTLE, --throttle THROTTLE
  -q, --quiet
  --debug
  --verbose
```

## Authentication support

- Plain text credentials in `~/.docker/config.json`

- `docker-credential-secretservice` from https://github.com/docker/docker-credential-helpers
