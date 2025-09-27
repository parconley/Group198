from __future__ import annotations

import argparse
import json
import logging
import shlex
import sys
from dataclasses import dataclass
from typing import Optional

from dss_common import (
    PROTOCOL_VERSION,
    RegistrationError,
    configure_logging,
    exchange_with_manager,
    generate_message_id,
    resolve_local_ipv4,
    validate_entity_name,
    validate_message_id,
    validate_ports,
    validate_retries,
    validate_timeout,
    deregister_entity,
)


@dataclass(frozen=True)
class RegisterArgs:
    disk_name: str
    manager_host: str
    manager_port: int
    management_port: int
    command_port: int
    local_ipv4: str
    message_id: str
    timeout: float
    retries: int


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DSS Disk Client")
    parser.add_argument("disk_name", help="Alphabetic identifier (≤15 chars)")
    parser.add_argument("manager_host", help="Manager host name or IPv4 address")
    parser.add_argument("manager_port", type=int, help="Manager UDP listen port")
    parser.add_argument("management_port", type=int, help="Disk management port (m-port)")
    parser.add_argument("command_port", type=int, help="Disk command port (c-port)")
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
        help="Explicit register_disk message identifier - must match design pattern",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Console log verbosity",
    )
    return parser.parse_args(argv)


def _configure_logging(level_name: str) -> None:
    configure_logging(level_name, "disk")


def _validate_cli(ns: argparse.Namespace) -> None:
    validate_entity_name(ns.disk_name, "disk_name")
    validate_ports(ns.management_port, ns.command_port)
    validate_timeout(ns.timeout)
    validate_retries(ns.retries)
    validate_message_id(ns.message_id)


def _prepare_register_args(ns: argparse.Namespace) -> RegisterArgs:
    message_id = ns.message_id or generate_message_id(ns.disk_name)
    local_ipv4 = resolve_local_ipv4(ns.ipv4_address, ns.manager_host, ns.manager_port)
    return RegisterArgs(
        disk_name=ns.disk_name,
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
        "message_type": "register_disk",
        "body": {
            "disk_name": args.disk_name,
            "ipv4_address": args.local_ipv4,
            "management_port": args.management_port,
            "command_port": args.command_port,
        },
    }
    return (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")


def register_disk(args: RegisterArgs) -> dict:
    payload = _build_register_payload(args)
    response = exchange_with_manager(
        action="register_disk",
        expected_response_type="register_disk_response",
        message_id=args.message_id,
        payload=payload,
        manager_host=args.manager_host,
        manager_port=args.manager_port,
        timeout=args.timeout,
        retries=args.retries,
    )

    status_code = response.get("status_code")
    if status_code not in {"SUCCESS", "FAILURE"}:
        raise RegistrationError("manager response missing status_code field")

    reason = response.get("reason")
    if status_code == "FAILURE":
        raise RegistrationError(f"Registration rejected: {reason or 'Unknown reason'}")

    state = None
    body = response.get("body")
    if isinstance(body, dict):
        state = body.get("state")

    logging.info(
        "register_disk SUCCESS disk=%s ip=%s mgmt=%d cmd=%d state=%s",
        args.disk_name,
        args.local_ipv4,
        args.management_port,
        args.command_port,
        state or "(unspecified)",
    )
    return response


def enter_service_loop(args: RegisterArgs, response: dict) -> int:
    logging.info("Registration complete; entering disk command loop")
    print("Available commands: deregister-disk, help")

    while True:
        try:
            line = input("disk> ")
        except EOFError:
            print()
            break

        if not line.strip():
            continue

        try:
            tokens = shlex.split(line)
        except ValueError as exc:
            logging.error("Unable to parse command: %s", exc)
            continue

        command = tokens[0].lower()

        if command in {"deregister-disk", "deregister"}:
            if _handle_deregister_disk(args):
                return 0
            continue

        if command in {"help", "?"}:
            print("Commands:\n  deregister-disk\n  help")
            continue

        if command in {"exit", "quit"}:
            logging.info("Use deregister-disk to cleanly exit the disk client")
            continue

        logging.error("Unknown command '%s'", tokens[0])

    return 0


def _handle_deregister_disk(args: RegisterArgs) -> bool:
    try:
        _, response = deregister_entity(
            "disk",
            args.disk_name,
            manager_host=args.manager_host,
            manager_port=args.manager_port,
            timeout=args.timeout,
            retries=args.retries,
        )
    except RegistrationError as exc:
        logging.error("deregister-disk exchange failed: %s", exc)
        return False
    except ValueError as exc:
        logging.error("deregister-disk invalid arguments: %s", exc)
        return False

    status_code = response.get("status_code")
    reason = response.get("reason")

    if status_code == "SUCCESS":
        logging.info("deregister-disk SUCCESS for %s", args.disk_name)
        print(json.dumps(response, indent=2))
        return True

    logging.error(
        "deregister-disk FAILURE for %s: %s",
        args.disk_name,
        reason or "Unknown reason",
    )
    print(json.dumps(response, indent=2))
    return False


def main(argv: Optional[list[str]] = None) -> int:
    ns = _parse_args(argv)
    _configure_logging(ns.log_level)

    try:
        _validate_cli(ns)
        reg_args = _prepare_register_args(ns)
        response = register_disk(reg_args)
    except ValueError as exc:
        logging.error("Invalid configuration: %s", exc)
        return 2
    except RegistrationError as exc:
        logging.error("register_disk failed: %s", exc)
        return 1

    return enter_service_loop(reg_args, response)


if __name__ == "__main__":
    sys.exit(main())
