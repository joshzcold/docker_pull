#!/usr/bin/env python3
"""
docker_pull.py, download docker images.

    Given a list of docker images to pull, download in a multiprocess pool and display structured progress for each
    download.
"""

import argparse
import atexit
import base64
import json
import logging
import multiprocessing
import os
import socket
import subprocess
import sys
import traceback
from dataclasses import dataclass
from multiprocessing import Manager, Pool, Queue
from multiprocessing.managers import RemoteError
from multiprocessing.pool import AsyncResult
from queue import Empty
from shutil import get_terminal_size, which
from typing import NotRequired, TypedDict

import requests
import urllib3
from requests.adapters import HTTPAdapter
from requests.compat import unquote, urlparse

HEADER = "\033[95m"
OKBLUE = "\033[94m"
OKCYAN = "\033[96m"
OKGREEN = "\033[92m"
INFO_GRAY = "\033[90m"
WARNING = "\033[93m"
FAIL = "\033[91m"
RESET = "\033[0m"
BOLD = "\033[1m"
UNDERLINE = "\033[4m"
ITALICS = "\033[3m"

CACHED_SECRET = {}


@dataclass
class ProgramArgs:
    """Typed program arguments for docker_pull."""

    images: list
    loglevel: int
    quiet: bool
    throttle: int = multiprocessing.cpu_count()


class TrackedProcess(TypedDict):
    """Class to represent 1 parallel process in progress."""

    queue: Queue
    proc: AsyncResult
    image: str
    downloaded: bool
    last_status: str


class ProgressDetail(TypedDict):
    """Represent progressDetail with current bytes downloaded and total of image."""

    current: int
    total: int


class DownloadProgress(TypedDict):
    """Represent json coming from container image download."""

    status: NotRequired[str]
    progressDetail: ProgressDetail
    progress: str
    id: str
    message: NotRequired[str]


parser = argparse.ArgumentParser()
_ = parser.add_argument("images", nargs="*", type=str)
_ = parser.add_argument("-x", "--throttle", dest="throttle", type=int)
_ = parser.add_argument("-q", "--quiet", dest="quiet", action="store_true", default=False)
_ = parser.add_argument(
    "--debug",
    dest="loglevel",
    action="store_const",
    const=logging.DEBUG,
    default=logging.WARNING,
)
_ = parser.add_argument("--verbose", dest="loglevel", action="store_const", const=logging.INFO)
args = parser.parse_args(namespace=ProgramArgs)
logging.basicConfig(level=args.loglevel)

UPDATE_IMAGES: list[str] = args.images
tasks: dict[str, TrackedProcess] = {}


# The following was adapted from some code from docker-py
# https://github.com/docker/docker-py/blob/master/docker/transport/unixconn.py
class UnixHTTPConnection(urllib3.connection.HTTPConnection, object):
    """
    Create an HTTP connection to a unix domain socket.

    :param unix_socket_url: A URL with a scheme of 'http+unix' and the
    netloc is a percent-encoded path to a unix domain socket. E.g.:
    'http+unix://%2Ftmp%2Fprofilesvc.sock/status/pid'
    """

    def __init__(self, unix_socket_url, timeout=60):
        """Initalize class."""
        super(UnixHTTPConnection, self).__init__("localhost", timeout=timeout)
        self.unix_socket_url = unix_socket_url
        self.timeout = timeout
        self.sock = None

    def __del__(self):  # base class does not have d'tor
        """Close socket on connection end."""
        if self.sock:
            self.sock.close()

    def connect(self):
        """Connect to Unix domain socket."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        socket_path = unquote(urlparse(self.unix_socket_url).netloc)
        sock.connect(socket_path)
        self.sock = sock


class UnixHTTPConnectionPool(urllib3.connectionpool.HTTPConnectionPool):
    def __init__(self, socket_path, timeout=60):
        super(UnixHTTPConnectionPool, self).__init__("localhost", timeout=timeout)
        self.socket_path = socket_path
        self.timeout = timeout

    def _new_conn(self):
        return UnixHTTPConnection(self.socket_path, self.timeout)


class UnixAdapter(HTTPAdapter):
    def __init__(self, timeout=60, pool_connections=25, *args, **kwargs):
        super(UnixAdapter, self).__init__(*args, **kwargs)
        self.timeout = timeout
        self.pools = urllib3._collections.RecentlyUsedContainer(pool_connections, dispose_func=lambda p: p.close())

    # Fix for requests 2.32.2+: https://github.com/psf/requests/pull/6710
    def get_connection_with_tls_context(self, request, verify, proxies=None, cert=None):
        return self.get_connection(request.url, proxies)

    def get_connection(self, url, proxies=None):
        proxies = proxies or {}
        proxy = proxies.get(urlparse(url.lower()).scheme)

        if proxy:
            raise ValueError("%s does not support specifying proxies" % self.__class__.__name__)

        with self.pools.lock:
            pool = self.pools.get(url)
            if pool:
                return pool

            pool = UnixHTTPConnectionPool(url, self.timeout)
            self.pools[url] = pool

        return pool

    def request_url(self, request, proxies):
        return request.path_url

    def close(self):
        self.pools.clear()


DEFAULT_SCHEME = "http+unix://"


# Provide a connection Class for connecting to Unix sockets
class UnixSession(requests.Session):
    def __init__(self, url_scheme=DEFAULT_SCHEME, *args, **kwargs):
        super(UnixSession, self).__init__(*args, **kwargs)
        self.mount(url_scheme, UnixAdapter())


def exit_handler():
    """Clean up steps on program exit."""
    # Show Cursor
    _ = sys.stdout.write("\033[?25h")
    # Enable line wrap
    _ = sys.stdout.write("\033[?7h")
    sys.stdout.flush()


def parse_image(full_image: str) -> tuple[str, str, str]:
    """Parse image url into parts."""
    full_image_parts = full_image.split("/")
    # Assume that images without a registry are coming from docker.io
    if len(full_image_parts) == 1:
        registry = "docker.io"
        repo_with_tag = full_image
    else:
        registry: str = full_image.split("/")[0]
        repo_with_tag: str = "/".join(full_image.split("/")[1:])
    repo, tag = repo_with_tag.split(":")
    return registry, repo, tag


def registry_auth_object(username: str, password: str, registry: str, email: str = ""):
    """
    Return docker credentials in format expected by docker engine api.

    https://docs.docker.com/reference/api/engine/version/v1.31/#section/Authentication.
    """
    return base64.b64encode(
        json.dumps({"username": username, "password": password, "email": email, "serveraddress": registry}).encode()
    ).decode()


def secret_lookup(registry: str) -> str | None:
    """
    Lookup a docker registry secret using multiple methods.

    Returns: base64 encoded string with username and password
    """
    if CACHED_SECRET and registry in CACHED_SECRET:
        return CACHED_SECRET[registry]
    docker_config_path = os.path.expanduser("~/.docker/config.json")
    docker_config_json = None
    try:
        if os.path.exists(docker_config_path):
            with open(docker_config_path, "r", encoding="utf-8") as f:
                docker_config_json = json.load(f)
    except Exception as ex:
        logging.error(f"Exception reading {docker_config_path}:")
        logging.error(traceback.print_exception(ex))
    if not docker_config_json:
        return None

    # Check for plain text credentials
    if (
        "auths" in docker_config_json
        and registry in docker_config_json["auths"]
        and "auth" in docker_config_json["auths"][registry]
    ):
        docker_secret = None
        try:
            stored_base64_plaintext_secret = docker_config_json["auths"][registry]["auth"]
            stored_plaintext_secret = base64.b64decode(stored_base64_plaintext_secret).decode()
            username, password = stored_plaintext_secret.split(":")

            docker_secret = registry_auth_object(username, password, registry)

            CACHED_SECRET[registry] = docker_secret
        except Exception as ex:
            logging.error(f"Exception parsing {docker_config_path}:")
            logging.error(traceback.print_exception(ex))
        return docker_secret

    # Check for lib secret service credentials
    if which("docker-credential-secretservice"):
        try:
            secret_service_cmd: subprocess.CompletedProcess[bytes] = subprocess.run(
                ["docker-credential-secretservice", "get"],
                input=registry.encode(),
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if secret_service_cmd.returncode == 0 and secret_service_cmd.stdout:
                secret_service_json = json.loads(secret_service_cmd.stdout)
                if "Username" in secret_service_json and "Secret" in secret_service_json:
                    username = secret_service_json["Username"]
                    password = secret_service_json["Secret"]
                    docker_secret = registry_auth_object(username, password, registry)
                    CACHED_SECRET[registry] = docker_secret
                    return docker_secret
            else:
                logging.debug("docker-credential-secretservice stderr: %s", secret_service_cmd.stderr)
                logging.debug("docker-credential-secretservice stdout: %s", secret_service_cmd.stdout)
        except Exception as ex:
            logging.error("Exception from docker-credential-secretservice")
            logging.error(traceback.print_exception(ex))
            return None

    return None


def pull_image(image: str, queue: Queue):  # noqa: C901,PLR0912 too-complex,too-many-branches
    """
    Call the docker unix socket to trigger a download of an image.

    The result of the pull should be a streaming json that contains the key 'progress'.
    Which we will display in the console.

    Args:
        image: full image tag.
        queue: Manager.Queue to send messages to.
    """
    try:
        registry, repo, tag = parse_image(image)
    except Exception as ex:
        queue.put((False, f"{FAIL}{image} had exception gathering facts {ex}{RESET}", True))
        return

    try:
        session = UnixSession()
        secret = secret_lookup(registry)
        client = session.post(
            "http+unix://%2Fvar%2Frun%2Fdocker.sock/v1.41/images/create",
            headers={"X-Registry-Auth": f"{secret}"},
            params={"fromImage": f"{registry}/{repo}", "tag": tag},
            stream=True,
        )
    except Exception as ex:
        queue.put((False, f"{FAIL}{image} had exception running request {ex}{RESET}", True))
        return

    line: bytes
    for line in client.iter_lines():
        if not line:
            break
        else:
            try:
                logging.debug(f"{image} -> {line.decode()}")
                json_progress: DownloadProgress = json.loads(line)

                # Error handle for error message coming back from docker daemon
                if "message" in json_progress:
                    message = json_progress["message"]
                    queue.put((False, f"{image};;{FAIL}{message}{RESET}", True))
                    session.close()
                    return
                elif "status" in json_progress:
                    status = json_progress["status"]
                    id = ""
                    if "id" in json_progress:
                        id = json_progress["id"]
                    match status:
                        case "Downloading" | "Extracting":
                            logging.debug(json_progress["progress"])
                            queue.put(
                                (
                                    True,
                                    f"{image};{INFO_GRAY}{id}{RESET};{json_progress['progress']}",
                                    False,
                                )
                            )
                        case _:
                            queue.put(
                                (
                                    True,
                                    f"{image};{INFO_GRAY}{id}{RESET};{status}",
                                    False,
                                )
                            )
            except KeyError as ex:
                logging.debug(str(ex))
                session.close()
                queue.put((False, f"{image};;{FAIL}{ex}{RESET}", True))
                return
            except json.JSONDecodeError as ex:
                logging.debug(str(ex))
                pass
            except TypeError as ex:
                logging.debug(str(ex))
                pass
            except RemoteError as ex:
                logging.debug(str(ex))
                pass
            except urllib3.exceptions.ProtocolError as ex:
                logging.debug(str(ex))
                pass
            except Exception as ex:
                logging.debug(str(ex))
                pass
    queue.put((False, f"{image};;✅", False))
    logging.debug("Close the socket")
    session.close()


def show_progress():  # noqa: C901,PLR0912 too-complex,too-many-branches
    """Get messages from Queue, parse information and display results interactively."""
    # Hide Cursor
    _ = sys.stdout.write("\033[?25l")
    # Disable line wrap
    _ = sys.stdout.write("\033[?7l")
    sys.stdout.flush()
    print()
    print(f"{OKGREEN}Downloading:{RESET}")
    # Reserve terminal space to show downlading images
    for image, _ in tasks.items():
        print()
    while True:
        all_done = True
        image: str
        tracked_progress: TrackedProcess
        output = {}
        for image, tracked_progress in tasks.items():
            try:
                is_running: bool
                message: str
                error: bool
                # Get last message from queue in each image
                is_running, message, error = tracked_progress["queue"].get(block=False)
                # If queue has error from tuple, stop the program and show error
                if error:
                    # Enable line wrap
                    _ = sys.stdout.write("\033[?7h")
                    if "no basic auth" in message:
                        raise Exception(
                            WARNING
                            + "\nNo required auth credentials for this registry\n"
                            + f"Try {OKCYAN}docker login <registry>{WARNING}\n"
                            + FAIL
                            + message
                            + RESET
                        )
                    raise Exception(f"Error coming back from docker: \n\t{message}")
                if message:
                    # save the last message, so we have something to display when tasks are done.
                    tracked_progress["last_status"] = message
                else:
                    message = tracked_progress["last_status"]

                output[image] = message

                if is_running:
                    # keep while loop going while we still have tasks reporting a running status
                    all_done = False
                else:
                    tracked_progress["downloaded"] = True
                    if args.quiet:
                        print(f"done. {image}")
                    continue
            except Empty:
                # get the last message saved since the queue is now empty
                if "last_status" in tracked_progress:
                    output[image] = tracked_progress["last_status"]
                # this means that we have tasks that haven't started yet
                if not tracked_progress["downloaded"]:
                    all_done = False
                    if "last_status" not in tracked_progress:
                        output[image] = f"{image};⌚;"
                pass
        if len(list(output.keys())) > 0:
            keys: list[str] = list(output.keys())
            # go back up to the beginning of the output in the terminal
            if not args.quiet:
                _ = sys.stdout.write(f"\x1b[{len(keys)}A")
            for key in keys:
                # split keys for formatted output
                img, id, status = output[key].split(";")
                term_size = get_terminal_size()[0]
                if not args.quiet:
                    _ = sys.stdout.write("\x1b[2K")  # delete existing line

                # Really small terminal
                if len(output[key]) > term_size + 60:
                    if status != "✅":
                        status = "..."
                    id = ""
                    img = img.split("/")[-1]
                    if not args.quiet:
                        _ = sys.stdout.write(f"{HEADER}{img}{RESET} {id} {status}\n")
                # Less small terminal
                elif len(output[key]) > term_size:
                    if status != "✅":
                        status = "..."
                    id = ""
                    if not args.quiet:
                        _ = sys.stdout.write(f"{HEADER}{img}{RESET} {id} {status}\n")
                # Full terminal
                elif not args.quiet:
                    _ = sys.stdout.write("{:<50} {:<10} {}".format(f"{HEADER}{img}{RESET}", id, status) + "\n")

        # if no condition kept the loop running then stop.
        if all_done:
            break

    print()
    print("Up to date 👍")


def start_job():
    """
    Start up tasks to download docker images.

    Each task gets a "Manager" queue to send messages asyncronously to the show_progress loop.
    A "Manager" queue is required since regular multiprocess child processes already use Queue under the hood,
        so we have to use a Manager queue as a proxy.
    """
    with Pool(processes=args.throttle) as pool:
        for image in UPDATE_IMAGES:
            manager = Manager()
            tp = TrackedProcess(queue=manager.Queue(), image=image)
            tasks[image] = tp
            tp["downloaded"] = False
            tasks[image]["proc"] = pool.apply_async(
                pull_image,
                args=(
                    tp["image"],
                    tp["queue"],
                ),
            )
        show_progress()
        pool.close()
        pool.join()


if __name__ == "__main__":
    _ = atexit.register(exit_handler)
    start_job()
