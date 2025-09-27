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
    deregister_entity,
    exchange_with_manager,
    generate_message_id,
    resolve_local_ipv4,
    validate_entity_name,
    validate_message_id,
    validate_ports,
    validate_retries,
    validate_timeout,
)


MIN_DSS_DISKS = 3
MIN_STRIPING_UNIT = 128
MAX_STRIPING_UNIT = 1024 * 1024


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
    configure_logging(level_name, "user")


def _validate_cli(ns: argparse.Namespace) -> None:
    validate_entity_name(ns.user_name, "user_name")
    validate_ports(ns.management_port, ns.command_port)
    validate_timeout(ns.timeout)
    validate_retries(ns.retries)
    validate_message_id(ns.message_id)


def _prepare_register_args(ns: argparse.Namespace) -> RegisterArgs:
    message_id = ns.message_id or generate_message_id(ns.user_name)
    local_ipv4 = resolve_local_ipv4(ns.ipv4_address, ns.manager_host, ns.manager_port)
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


def register_user(args: RegisterArgs) -> dict:
    payload = _build_register_payload(args)
    response = exchange_with_manager(
        action="register_user",
        expected_response_type="register_user_response",
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

    logging.info(
        "register_user SUCCESS user=%s ip=%s mgmt=%d cmd=%d",
        args.user_name,
        args.local_ipv4,
        args.management_port,
        args.command_port,
    )
    return response


def _is_power_of_two(value: int) -> bool:
    return value > 0 and (value & (value - 1)) == 0


def _handle_configure_dss(args: RegisterArgs, tokens: list[str]) -> None:
    if len(tokens) != 4:
        print("Usage: configure-dss <dss-name> <n> <striping-unit>")
        return

    _, dss_name, n_text, striping_text = tokens

    try:
        validate_entity_name(dss_name, "dss_name")
    except ValueError as exc:
        logging.error("Invalid dss_name: %s", exc)
        return

    try:
        n_value = int(n_text)
    except ValueError:
        logging.error("n must be an integer >= %d", MIN_DSS_DISKS)
        return

    if n_value < MIN_DSS_DISKS:
        logging.error("n must be at least %d", MIN_DSS_DISKS)
        return

    try:
        striping_unit = int(striping_text)
    except ValueError:
        logging.error(
            "striping-unit must be an integer between %d and %d",
            MIN_STRIPING_UNIT,
            MAX_STRIPING_UNIT,
        )
        return

    if (
        striping_unit < MIN_STRIPING_UNIT
        or striping_unit > MAX_STRIPING_UNIT
        or not _is_power_of_two(striping_unit)
    ):
        logging.error(
            "striping-unit must be a power of two between %d and %d",
            MIN_STRIPING_UNIT,
            MAX_STRIPING_UNIT,
        )
        return

    message_id = generate_message_id(args.user_name)
    message = {
        "version": PROTOCOL_VERSION,
        "message_id": message_id,
        "message_type": "configure_dss",
        "body": {
            "user_name": args.user_name,
            "dss_name": dss_name,
            "n": n_value,
            "striping_unit": striping_unit,
        },
    }
    payload = (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        response = exchange_with_manager(
            action="configure_dss",
            expected_response_type="configure_dss_response",
            message_id=message_id,
            payload=payload,
            manager_host=args.manager_host,
            manager_port=args.manager_port,
            timeout=args.timeout,
            retries=args.retries,
        )
    except RegistrationError as exc:
        logging.error("configure-dss exchange failed: %s", exc)
        return

    status_code = response.get("status_code")
    reason = response.get("reason")

    if status_code == "SUCCESS":
        body = response.get("body")
        if not isinstance(body, dict):
            body = {}
        disks = body.get("disks")
        disks_text = ", ".join(disks) if isinstance(disks, list) else "(unspecified)"
        logging.info(
            "configure-dss SUCCESS dss=%s n=%d striping=%d disks=%s",
            dss_name,
            n_value,
            striping_unit,
            disks_text,
        )
    else:
        logging.warning(
            "configure-dss FAILURE dss=%s n=%d striping=%d reason=%s",
            dss_name,
            n_value,
            striping_unit,
            reason or "Unknown reason",
        )

    print(json.dumps(response, indent=2))


def _handle_deregister_user(args: RegisterArgs) -> bool:
    try:
        _, response = deregister_entity(
            "user",
            args.user_name,
            manager_host=args.manager_host,
            manager_port=args.manager_port,
            timeout=args.timeout,
            retries=args.retries,
        )
    except RegistrationError as exc:
        logging.error("deregister-user exchange failed: %s", exc)
        return False
    except ValueError as exc:
        logging.error("deregister-user invalid arguments: %s", exc)
        return False

    status_code = response.get("status_code")
    reason = response.get("reason")

    if status_code == "SUCCESS":
        logging.info("deregister-user SUCCESS for %s", args.user_name)
        print(json.dumps(response, indent=2))
        return True

    logging.error(
        "deregister-user FAILURE for %s: %s",
        args.user_name,
        reason or "Unknown reason",
    )
    print(json.dumps(response, indent=2))
    return False


def enter_command_loop(args: RegisterArgs, response: dict) -> int:
    logging.info("Registration complete; entering command loop")
    print(
        "Available commands: configure-dss <dss-name> <n> <striping-unit>, deregister-user, help"
    )

    while True:
        try:
            line = input("user> ")
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

        if command == "configure-dss":
            _handle_configure_dss(args, tokens)
            continue

        if command in {"deregister-user", "deregister"}:
            if _handle_deregister_user(args):
                return 0
            continue

        if command in {"help", "?"}:
            print(
                "Commands:\n"
                "  configure-dss <dss-name> <n> <striping-unit>\n"
                "  deregister-user\n"
                "  help"
            )
            continue

        if command in {"exit", "quit"}:
            logging.info("Use deregister-user to cleanly exit the client")
            continue

        logging.error("Unknown command '%s'", tokens[0])

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
