"""
Distributed Storage System (DSS) Manager

Listens on a UDP port for newline-delimited JSON (NDJSON) messages and
enforces the envelope specified in the design document for `register_user`.

Envelope schema (single JSON object per line):
{
  "version": 1,
  "message_id": "<role>-<pid>-<timestamp_ms>-<counter>",
  "message_type": "register_user",
  "body": {
    "user_name": "<A-Za-z string, max 15>",
    "ipv4_address": "<dotted-quad>",
    "management_port": <int>,
    "command_port": <int>
  }
}

Response schema:
{
  "version": 1,
  "message_id": "<mirrors request>",
  "message_type": "register_user_response",
  "status_code": "SUCCESS" | "FAILURE",
  "reason": null | "<explanation on FAILURE>"
}
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import logging
import signal
import socket
import sys
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

PROTOCOL_VERSION = 1
VALID_PORT_MIN = 21200
VALID_PORT_MAX = 21299
MESSAGE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+-\d+-\d+-\d{5,}$")


@dataclass(frozen=True)
class UserRecord:
    user_name: str
    ipv4_address: str
    management_port: int
    command_port: int


class ManagerServer:
    """UDP server for DSS manager."""

    def __init__(self, listen_port: int):
        self.listen_port: int = listen_port
        # user_name -> record
        self.users: Dict[str, UserRecord] = {}
        # port -> owner identifier (e.g., "user:<user_name>")
        self.claimed_ports: Dict[int, str] = {}

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Allow quick restart
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except OSError:
            pass
        self.sock.bind(("0.0.0.0", self.listen_port))

    def serve_forever(self) -> None:
        logging.info("Manager listening on UDP port %s", self.listen_port)
        while True:
            try:
                data, addr = self.sock.recvfrom(64 * 1024)
            except OSError as exc:
                logging.error("Socket error: %s", exc)
                continue

            if not data:
                continue

            text = None
            try:
                text = data.decode("utf-8", errors="strict")
            except UnicodeDecodeError:
                logging.warning("Received undecodable bytes from %s; dropping", addr)
                continue

            # Each UDP datagram may carry one or more NDJSON lines. Process each non-empty line.
            for line in (ln for ln in text.splitlines() if ln.strip()):
                self._process_line(line, addr)

    def _process_line(self, line: str, addr: Tuple[str, int]) -> None:
        try:
            message = json.loads(line)
            if not isinstance(message, dict):
                raise ValueError("Top-level JSON must be an object")
        except Exception as exc:
            logging.warning("Invalid JSON from %s: %s", addr, exc)
            # Cannot mirror message_id; send minimal failure with synthetic id
            self._send_response(
                addr,
                message_type="register_user_response",
                message_id="INVALID",
                status_code="FAILURE",
                reason="Malformed JSON",
            )
            return

        message_id = message.get("message_id")
        message_id_for_response = (
            message_id if isinstance(message_id, str) and message_id else "MISSING"
        )

        version = message.get("version")
        if version != PROTOCOL_VERSION:
            logging.info(
                "Unsupported protocol version %s from %s", version, addr
            )
            self._send_response(
                addr,
                message_type="register_user_response",
                message_id=message_id_for_response,
                status_code="FAILURE",
                reason="Unsupported protocol version",
            )
            return

        message_type = message.get("message_type")
        body = message.get("body")

        if not isinstance(message_id, str) or not message_id:
            logging.info("Missing or invalid message_id from %s", addr)
            self._send_response(
                addr,
                message_type="register_user_response",
                message_id="MISSING",
                status_code="FAILURE",
                reason="message_id must be a non-empty string",
            )
            return

        if not MESSAGE_ID_PATTERN.match(message_id):
            logging.info("Invalid message_id format '%s' from %s", message_id, addr)
            self._send_response(
                addr,
                message_type="register_user_response",
                message_id=message_id,
                status_code="FAILURE",
                reason="message_id must follow <role>-<pid>-<timestamp_ms>-<counter>",
            )
            return

        if message_type != "register_user":
            logging.info("Unknown message_type '%s' from %s", message_type, addr)
            self._send_response(
                addr,
                message_type="register_user_response",
                message_id=message_id,
                status_code="FAILURE",
                reason="Unsupported message_type",
            )
            return

        self.handle_register_user(message_id, body, addr)

    def handle_register_user(self, message_id: str, body: Any, addr: Tuple[str, int]) -> None:
        """Validate and register a user. Sends response to addr."""
        # Validate base fields
        error = self._validate_register_body(body)
        if error is not None:
            logging.info("register_user FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="register_user_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        assert isinstance(body, dict)
        user_name = body["user_name"]
        ipv4_address = body["ipv4_address"]
        management_port = int(body["management_port"])  # cast after validation
        command_port = int(body["command_port"])

        # Name uniqueness
        if user_name in self.users:
            error = f"user_name '{user_name}' is already registered"
            logging.info("register_user FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="register_user_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        # Port availability (cluster-wide) and per-process uniqueness
        if management_port == command_port:
            error = "management_port and command_port must be different"
            logging.info("register_user FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="register_user_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        for port in (management_port, command_port):
            owner = self.claimed_ports.get(port)
            if owner is not None:
                error = f"port {port} is already claimed by {owner}"
                logging.info("register_user FAILURE from %s: %s", addr, error)
                self._send_response(
                    addr,
                    message_type="register_user_response",
                    message_id=message_id,
                    status_code="FAILURE",
                    reason=error,
                )
                return

        # Success path: store state
        record = UserRecord(
            user_name=user_name,
            ipv4_address=ipv4_address,
            management_port=management_port,
            command_port=command_port,
        )
        self.users[user_name] = record
        self.claimed_ports[management_port] = f"user:{user_name}"
        self.claimed_ports[command_port] = f"user:{user_name}"

        logging.info(
            "register_user SUCCESS user=%s ip=%s mgmt=%d cmd=%d from %s",
            user_name,
            ipv4_address,
            management_port,
            command_port,
            addr,
        )

        self._send_response(
            addr,
            message_type="register_user_response",
            message_id=message_id,
            status_code="SUCCESS",
            reason=None,
        )

    def _validate_register_body(self, body: Any) -> Optional[str]:
        if not isinstance(body, dict):
            return "body must be an object"

        required = ["user_name", "ipv4_address", "management_port", "command_port"]
        for key in required:
            if key not in body:
                return f"missing field: {key}"

        user_name = body["user_name"]
        if not isinstance(user_name, str):
            return "user_name must be a string"
        if len(user_name) == 0 or len(user_name) > 15:
            return "user_name length must be 1..15"
        if not user_name.isalpha():
            return "user_name must contain only alphabetic characters"

        ipv4 = body["ipv4_address"]
        if not isinstance(ipv4, str):
            return "ipv4_address must be a string"
        try:
            ipaddress.IPv4Address(ipv4)
        except Exception:
            return "ipv4_address must be a valid IPv4 address"

        try:
            mgmt_port = int(body["management_port"])  # may raise
            cmd_port = int(body["command_port"])  # may raise
        except Exception:
            return "management_port and command_port must be integers"

        for label, port in ("management_port", mgmt_port), ("command_port", cmd_port):
            if port < VALID_PORT_MIN or port > VALID_PORT_MAX:
                return f"{label} must be in range {VALID_PORT_MIN}-{VALID_PORT_MAX}"

        return None

    def _send_response(
        self,
        addr: Tuple[str, int],
        *,
        message_type: str,
        message_id: str,
        status_code: str,
        reason: Optional[str],
        body: Optional[Dict[str, Any]] = None,
    ) -> None:
        response: Dict[str, Any] = {
            "version": PROTOCOL_VERSION,
            "message_id": message_id,
            "message_type": message_type,
            "status_code": status_code,
            "reason": reason,
        }
        if body is not None:
            response["body"] = body

        payload = json.dumps(response, separators=(",", ":")) + "\n"
        try:
            self.sock.sendto(payload.encode("utf-8"), addr)
        except OSError as exc:
            logging.error("Failed to send response to %s: %s", addr, exc)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DSS Manager Server")
    parser.add_argument(
        "listen_port",
        type=int,
        help="UDP port to listen on (expected in range %d-%d)" % (VALID_PORT_MIN, VALID_PORT_MAX),
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
        format="%(asctime)s [%(levelname)s] manager: %(message)s",
    )


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.log_level)

    server = ManagerServer(args.listen_port)

    # Graceful shutdown on Ctrl+C
    def _handle_sigint(signum, frame):
        logging.info("Shutting down manager...")
        try:
            server.sock.close()
        finally:
            sys.exit(0)

    try:
        signal.signal(signal.SIGINT, _handle_sigint)
    except Exception:
        pass

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        _handle_sigint(signal.SIGINT, None)
    return 0


if __name__ == "__main__":
    sys.exit(main())


