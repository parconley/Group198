from __future__ import annotations

import ipaddress
import itertools
import json
import logging
import os
import re
import socket
import time
from typing import Optional

PROTOCOL_VERSION = 1
VALID_PORT_MIN = 21200
VALID_PORT_MAX = 21299
MAX_DATAGRAM_SIZE = 64 * 1024
MESSAGE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+-\d+-\d+-\d{5,}$")
_MESSAGE_COUNTER = itertools.count(1)


class RegistrationError(Exception):
    """Raised when registration-related exchanges fail."""


def configure_logging(level_name: str, component: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format=f"%(asctime)s [%(levelname)s] {component}: %(message)s",
    )


def validate_entity_name(name: str, label: str) -> None:
    if not name.isalpha():
        raise ValueError(f"{label} must contain only alphabetic characters")
    if len(name) == 0 or len(name) > 15:
        raise ValueError(f"{label} length must be between 1 and 15 characters")


def validate_ports(management_port: int, command_port: int) -> None:
    if management_port == command_port:
        raise ValueError("management_port and command_port must differ")

    for label, value in (
        ("management_port", management_port),
        ("command_port", command_port),
    ):
        if value < VALID_PORT_MIN or value > VALID_PORT_MAX:
            raise ValueError(
                f"{label} must be within the range {VALID_PORT_MIN}-{VALID_PORT_MAX}"
            )


def validate_timeout(timeout: float) -> None:
    if timeout <= 0:
        raise ValueError("timeout must be positive")


def validate_retries(retries: int) -> None:
    if retries < 0:
        raise ValueError("retries must be zero or a positive integer")


def validate_message_id(message_id: Optional[str]) -> None:
    if message_id and not MESSAGE_ID_PATTERN.match(message_id):
        raise ValueError(
            "message_id must follow <role>-<pid>-<timestamp_ms>-<counter>"
        )


def resolve_local_ipv4(
    explicit_ipv4: Optional[str], manager_host: str, manager_port: int
) -> str:
    if explicit_ipv4:
        try:
            ipaddress.IPv4Address(explicit_ipv4)
        except Exception as exc:  # pragma: no cover - defensive path
            raise ValueError("--ipv4-address must be a valid IPv4 address") from exc
        return explicit_ipv4

    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect((manager_host, manager_port))
        local_ip = probe.getsockname()[0]
        ipaddress.IPv4Address(local_ip)
        return local_ip
    except Exception as exc:
        raise ValueError(
            "Unable to determine local IPv4 address; specify via --ipv4-address"
        ) from exc
    finally:
        probe.close()


def generate_message_id(role: str) -> str:
    pid = os.getpid()
    timestamp_ms = int(time.time() * 1000)
    counter = next(_MESSAGE_COUNTER)
    return f"{role}-{pid}-{timestamp_ms}-{counter:05d}"


def exchange_with_manager(
    *,
    action: str,
    expected_response_type: str,
    message_id: str,
    payload: bytes,
    manager_host: str,
    manager_port: int,
    timeout: float,
    retries: int,
) -> dict:
    manager_addr = (manager_host, manager_port)
    attempts = retries + 1

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(timeout)
        for attempt in range(1, attempts + 1):
            logging.info(
                "%s attempt %d/%d to %s:%d",
                action,
                attempt,
                attempts,
                manager_host,
                manager_port,
            )
            try:
                sock.sendto(payload, manager_addr)
            except OSError as exc:
                raise RegistrationError(f"failed to send {action}: {exc}") from exc

            try:
                data, addr = sock.recvfrom(MAX_DATAGRAM_SIZE)
            except socket.timeout:
                if attempt < attempts:
                    logging.warning("Timed out waiting for manager response; retrying")
                    continue
                raise RegistrationError("Timed out waiting for manager response")
            except OSError as exc:
                raise RegistrationError(
                    f"socket error waiting for response: {exc}"
                ) from exc

            if addr[1] != manager_port:
                logging.debug("Ignoring packet from unexpected sender %s", addr)
                continue

            try:
                decoded = data.decode("utf-8", errors="strict")
            except UnicodeDecodeError:
                logging.warning("Received undecodable response; ignoring")
                continue

            for line in (ln for ln in decoded.splitlines() if ln.strip()):
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    logging.warning("Received malformed JSON line: %s", line)
                    continue

                if message.get("version") != PROTOCOL_VERSION:
                    logging.debug(
                        "Ignoring response with protocol version %s", message.get("version")
                    )
                    continue

                if message.get("message_type") != expected_response_type:
                    logging.debug("Ignoring message_type=%s", message.get("message_type"))
                    continue

                if message.get("message_id") != message_id:
                    logging.debug(
                        "Ignoring response with mismatched message_id %s",
                        message.get("message_id"),
                    )
                    continue

                return message

            logging.warning("No usable response lines received; retrying")

    raise RegistrationError(f"Did not receive a valid {expected_response_type}")
