from __future__ import annotations

import argparse
import itertools
import ipaddress
import json
import logging
import os
import re
import socket
import sys
import time
from dataclasses import dataclass
from typing import Optional

PROTOCOL_VERSION = 1
VALID_PORT_MIN = 21200
VALID_PORT_MAX = 21299
MAX_DATAGRAM_SIZE = 64 * 1024
MESSAGE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+-\d+-\d+-\d{5,}$")
_MESSAGE_COUNTER = itertools.count(1)


class RegistrationError(Exception):
    """Raised when the register_user exchange fails."""


@dataclass(frozen=True)
class RegisterArgs:
    user_name: str
    manager_host: str
    manager_port: int
    management_port: int
    command_port: int
    local_ipv4: str
    message_id: str
    timeout: float
    retries: int


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DSS User Client")
    parser.add_argument("user_name", help="Alphabetic identifier (≤15 chars)")
    parser.add_argument("manager_host", help="Manager host name or IPv4 address")
    parser.add_argument("manager_port", type=int, help="Manager UDP listen port")
    parser.add_argument("management_port", type=int, help="User management port (m-port)")
    parser.add_argument("command_port", type=int, help="User command port (c-port)")
    parser.add_argument(
        "--ipv4-address",
        dest="ipv4_address",
        help="Override auto-detected local IPv4 address",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=3.0,
        help="Seconds to wait for a manager response before retrying",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=0,
        help="Number of additional attempts on timeout",
    )
    parser.add_argument(
        "--message-id",
        dest="message_id",
        help="Explicit register_user message identifier (must match design pattern)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Console log verbosity",
    )
    return parser.parse_args(argv)


def _configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] user: %(message)s",
    )


def _validate_cli(ns: argparse.Namespace) -> None:
    name = ns.user_name
    if not name.isalpha():
        raise ValueError("user_name must contain only alphabetic characters")
    if len(name) == 0 or len(name) > 15:
        raise ValueError("user_name length must be between 1 and 15 characters")

    if ns.management_port == ns.command_port:
        raise ValueError("management_port and command_port must differ")

    for label, value in (
        ("management_port", ns.management_port),
        ("command_port", ns.command_port),
    ):
        if value < VALID_PORT_MIN or value > VALID_PORT_MAX:
            raise ValueError(
                f"{label} must be within the range {VALID_PORT_MIN}-{VALID_PORT_MAX}"
            )

    if ns.timeout <= 0:
        raise ValueError("timeout must be positive")

    if ns.retries < 0:
        raise ValueError("retries must be zero or a positive integer")

    if ns.message_id and not MESSAGE_ID_PATTERN.match(ns.message_id):
        raise ValueError(
            "message_id must follow <role>-<pid>-<timestamp_ms>-<counter>"
        )


def _resolve_local_ipv4(explicit_ipv4: Optional[str], manager_host: str, manager_port: int) -> str:
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


def _generate_message_id(role: str) -> str:
    pid = os.getpid()
    timestamp_ms = int(time.time() * 1000)
    counter = next(_MESSAGE_COUNTER)
    return f"{role}-{pid}-{timestamp_ms}-{counter:05d}"


def _prepare_register_args(ns: argparse.Namespace) -> RegisterArgs:
    message_id = ns.message_id or _generate_message_id(ns.user_name)
    local_ipv4 = _resolve_local_ipv4(ns.ipv4_address, ns.manager_host, ns.manager_port)
    return RegisterArgs(
        user_name=ns.user_name,
        manager_host=ns.manager_host,
        manager_port=ns.manager_port,
        management_port=ns.management_port,
        command_port=ns.command_port,
        local_ipv4=local_ipv4,
        message_id=message_id,
        timeout=ns.timeout,
        retries=ns.retries,
    )


def _build_register_payload(args: RegisterArgs) -> bytes:
    message = {
        "version": PROTOCOL_VERSION,
        "message_id": args.message_id,
        "message_type": "register_user",
        "body": {
            "user_name": args.user_name,
            "ipv4_address": args.local_ipv4,
            "management_port": args.management_port,
            "command_port": args.command_port,
        },
    }
    return (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")


def _send_and_receive(args: RegisterArgs, payload: bytes) -> dict:
    manager_addr = (args.manager_host, args.manager_port)
    attempts = args.retries + 1

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(args.timeout)
        for attempt in range(1, attempts + 1):
            logging.info(
                "register_user attempt %d/%d to %s:%d", attempt, attempts, *manager_addr
            )
            try:
                sock.sendto(payload, manager_addr)
            except OSError as exc:
                raise RegistrationError(f"failed to send register_user: {exc}") from exc

            try:
                data, addr = sock.recvfrom(MAX_DATAGRAM_SIZE)
            except socket.timeout:
                if attempt < attempts:
                    logging.warning("Timed out waiting for manager response; retrying")
                    continue
                raise RegistrationError("Timed out waiting for manager response")
            except OSError as exc:
                raise RegistrationError(f"socket error waiting for response: {exc}") from exc

            if addr[1] != args.manager_port:
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

                if message.get("message_type") != "register_user_response":
                    logging.debug("Ignoring message_type=%s", message.get("message_type"))
                    continue

                if message.get("message_id") != args.message_id:
                    logging.debug(
                        "Ignoring response with mismatched message_id %s",
                        message.get("message_id"),
                    )
                    continue

                return message

            logging.warning("No usable response lines received; retrying")

    raise RegistrationError("Did not receive a valid register_user_response")


def register_user(args: RegisterArgs) -> dict:
    payload = _build_register_payload(args)
    response = _send_and_receive(args, payload)

    status_code = response.get("status_code")
    if status_code not in {"SUCCESS", "FAILURE"}:
        raise RegistrationError("manager response missing status_code field")

    reason = response.get("reason")
    if status_code == "FAILURE":
        raise RegistrationError(f"Registration rejected: {reason or 'Unknown reason'}")

    logging.info(
        "register_user SUCCESS user=%s ip=%s mgmt=%d cmd=%d",
        args.user_name,
        args.local_ipv4,
        args.management_port,
        args.command_port,
    )
    return response


def enter_command_loop(args: RegisterArgs, response: dict) -> int:
    logging.info("Entering command loop (not yet implemented)")
    # Placeholder for upcoming user command handling implementation.
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    ns = _parse_args(argv)
    _configure_logging(ns.log_level)

    try:
        _validate_cli(ns)
        reg_args = _prepare_register_args(ns)
        response = register_user(reg_args)
    except ValueError as exc:
        logging.error("Invalid configuration: %s", exc)
        return 2
    except RegistrationError as exc:
        logging.error("register_user failed: %s", exc)
        return 1

    return enter_command_loop(reg_args, response)


if __name__ == "__main__":
    sys.exit(main())
