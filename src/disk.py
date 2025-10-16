from __future__ import annotations

import argparse
import base64
import json
import logging
import shlex
import socket
import sys
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from disk_storage import BlockRecord, DiskStorage
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


class DiskServer:
    """Multi-threaded disk server with command port for P2P messages."""

    def __init__(self, args: RegisterArgs):
        self.args = args
        self.storage = DiskStorage()
        self.running = threading.Event()
        self.running.set()

        # Create command port socket
        self.command_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.command_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except OSError:
            pass
        self.command_sock.bind(("0.0.0.0", args.command_port))
        self.command_sock.settimeout(1.0)  # For periodic shutdown checks

        # Start command port thread
        self.command_thread = threading.Thread(target=self._command_port_loop, daemon=True)
        self.command_thread.start()

        logging.info(
            "Disk server started: command_port=%d storage_initialized=True",
            args.command_port
        )

    def shutdown(self):
        """Stop the command port thread and close sockets."""
        self.running.clear()
        if self.command_thread.is_alive():
            self.command_thread.join(timeout=2.0)
        try:
            self.command_sock.close()
        except:
            pass
        logging.info("Disk server shutdown complete")

    def _command_port_loop(self):
        """Thread that handles P2P messages on the command port."""
        logging.info("Command port thread started on port %d", self.args.command_port)
        while self.running.is_set():
            try:
                data, addr = self.command_sock.recvfrom(64 * 1024)
            except socket.timeout:
                continue
            except OSError as exc:
                if self.running.is_set():
                    logging.error("Command port socket error: %s", exc)
                break

            if not data:
                continue

            try:
                text = data.decode("utf-8", errors="strict")
            except UnicodeDecodeError:
                logging.warning("Received undecodable bytes on command port from %s", addr)
                continue

            # Process each NDJSON line
            for line in (ln for ln in text.splitlines() if ln.strip()):
                self._process_p2p_message(line, addr)

        logging.info("Command port thread exiting")

    def _process_p2p_message(self, line: str, addr: Tuple[str, int]):
        """Parse and handle a P2P message from a user."""
        try:
            message = json.loads(line)
            if not isinstance(message, dict):
                raise ValueError("Top-level JSON must be an object")
        except Exception as exc:
            logging.warning("Invalid JSON on command port from %s: %s", addr, exc)
            return

        message_id = message.get("message_id")
        message_type = message.get("message_type")
        body = message.get("body")

        if not isinstance(message_id, str) or not isinstance(message_type, str):
            logging.warning("Invalid message_id or message_type from %s", addr)
            return

        if message_type == "write_block":
            self._handle_write_block(message_id, body, addr)
        elif message_type == "read_block":
            self._handle_read_block(message_id, body, addr)
        elif message_type == "fail":
            self._handle_fail(message_id, body, addr)
        elif message_type == "read_block_for_recovery":
            # Same as read_block but logged differently
            self._handle_read_block(message_id, body, addr, recovery=True)
        elif message_type == "write_recovered_block":
            # Same as write_block but logged differently
            self._handle_write_block(message_id, body, addr, recovery=True)
        elif message_type == "delete_all":
            self._handle_delete_all(message_id, body, addr)
        else:
            logging.warning("Unknown P2P message_type '%s' from %s", message_type, addr)
            self._send_p2p_response(
                addr,
                message_id=message_id,
                message_type=f"{message_type}_response",
                status_code="FAILURE",
                reason="Unsupported message_type"
            )

    def _handle_write_block(self, message_id: str, body: Any, addr: Tuple[str, int], recovery: bool = False):
        """Handle write_block or write_recovered_block request."""
        if not isinstance(body, dict):
            self._send_p2p_response(
                addr,
                message_id=message_id,
                message_type="write_recovered_block_response" if recovery else "write_block_response",
                status_code="FAILURE",
                reason="body must be an object"
            )
            return

        required = ["dss_name", "file_name", "file_size", "owner", "stripe_number", "block_type", "block_data_base64"]
        for key in required:
            if key not in body:
                self._send_p2p_response(
                    addr,
                    message_id=message_id,
                    message_type="write_recovered_block_response" if recovery else "write_block_response",
                    status_code="FAILURE",
                    reason=f"missing field: {key}"
                )
                return

        try:
            block_data = base64.b64decode(body["block_data_base64"])
        except Exception as exc:
            self._send_p2p_response(
                addr,
                message_id=message_id,
                message_type="write_recovered_block_response" if recovery else "write_block_response",
                status_code="FAILURE",
                reason=f"invalid base64: {exc}"
            )
            return

        record = BlockRecord(
            dss_name=body["dss_name"],
            file_name=body["file_name"],
            file_size=int(body["file_size"]),
            owner=body["owner"],
            stripe_number=int(body["stripe_number"]),
            block_type=body["block_type"],
            block_data=block_data
        )

        self.storage.write_block(record)

        logging.info(
            "%s SUCCESS dss=%s file=%s stripe=%d type=%s size=%d from=%s",
            "write_recovered_block" if recovery else "write_block",
            record.dss_name,
            record.file_name,
            record.stripe_number,
            record.block_type,
            len(block_data),
            addr
        )

        self._send_p2p_response(
            addr,
            message_id=message_id,
            message_type="write_recovered_block_response" if recovery else "write_block_response",
            status_code="SUCCESS",
            reason=None
        )

    def _handle_read_block(self, message_id: str, body: Any, addr: Tuple[str, int], recovery: bool = False):
        """Handle read_block or read_block_for_recovery request."""
        if not isinstance(body, dict):
            self._send_p2p_response(
                addr,
                message_id=message_id,
                message_type="read_block_response",
                status_code="FAILURE",
                reason="body must be an object"
            )
            return

        required = ["dss_name", "file_name", "stripe_number"]
        for key in required:
            if key not in body:
                self._send_p2p_response(
                    addr,
                    message_id=message_id,
                    message_type="read_block_response",
                    status_code="FAILURE",
                    reason=f"missing field: {key}"
                )
                return

        dss_name = body["dss_name"]
        file_name = body["file_name"]
        stripe_number = int(body["stripe_number"])

        record = self.storage.read_block(dss_name, file_name, stripe_number)

        if record is None:
            logging.warning(
                "%s FAILURE dss=%s file=%s stripe=%d: block not found from=%s",
                "read_block_for_recovery" if recovery else "read_block",
                dss_name,
                file_name,
                stripe_number,
                addr
            )
            self._send_p2p_response(
                addr,
                message_id=message_id,
                message_type="read_block_response",
                status_code="FAILURE",
                reason="Block not found"
            )
            return

        block_data_base64 = base64.b64encode(record.block_data).decode("ascii")

        logging.info(
            "%s SUCCESS dss=%s file=%s stripe=%d type=%s size=%d from=%s",
            "read_block_for_recovery" if recovery else "read_block",
            record.dss_name,
            record.file_name,
            record.stripe_number,
            record.block_type,
            len(record.block_data),
            addr
        )

        self._send_p2p_response(
            addr,
            message_id=message_id,
            message_type="read_block_response",
            status_code="SUCCESS",
            reason=None,
            body={
                "block_type": record.block_type,
                "block_data_base64": block_data_base64
            }
        )

    def _handle_fail(self, message_id: str, body: Any, addr: Tuple[str, int]):
        """Handle fail request - delete all blocks for the specified DSS."""
        if not isinstance(body, dict) or "dss_name" not in body:
            self._send_p2p_response(
                addr,
                message_id=message_id,
                message_type="fail_response",
                status_code="FAILURE",
                reason="missing field: dss_name"
            )
            return

        dss_name = body["dss_name"]
        blocks_deleted, files_deleted = self.storage.delete_dss(dss_name)

        logging.info(
            "fail SUCCESS dss=%s blocks_deleted=%d files_deleted=%d from=%s",
            dss_name,
            blocks_deleted,
            files_deleted,
            addr
        )

        self._send_p2p_response(
            addr,
            message_id=message_id,
            message_type="fail_response",
            status_code="SUCCESS",
            reason=None,
            body={"blocks_deleted": blocks_deleted}
        )

    def _handle_delete_all(self, message_id: str, body: Any, addr: Tuple[str, int]):
        """Handle delete_all request - delete all blocks for a DSS (used in decommission)."""
        if not isinstance(body, dict) or "dss_name" not in body:
            self._send_p2p_response(
                addr,
                message_id=message_id,
                message_type="delete_all_response",
                status_code="FAILURE",
                reason="missing field: dss_name"
            )
            return

        dss_name = body["dss_name"]
        blocks_deleted, files_deleted = self.storage.delete_dss(dss_name)

        logging.info(
            "delete_all SUCCESS dss=%s blocks_deleted=%d files_deleted=%d from=%s",
            dss_name,
            blocks_deleted,
            files_deleted,
            addr
        )

        self._send_p2p_response(
            addr,
            message_id=message_id,
            message_type="delete_all_response",
            status_code="SUCCESS",
            reason=None,
            body={
                "blocks_deleted": blocks_deleted,
                "files_deleted": files_deleted
            }
        )

    def _send_p2p_response(
        self,
        addr: Tuple[str, int],
        *,
        message_id: str,
        message_type: str,
        status_code: str,
        reason: Optional[str],
        body: Optional[Dict[str, Any]] = None
    ):
        """Send a P2P response message."""
        response: Dict[str, Any] = {
            "version": PROTOCOL_VERSION,
            "message_id": message_id,
            "message_type": message_type,
            "status_code": status_code,
            "reason": reason
        }
        if body is not None:
            response["body"] = body

        payload = json.dumps(response, separators=(",", ":")) + "\n"
        try:
            self.command_sock.sendto(payload.encode("utf-8"), addr)
        except OSError as exc:
            logging.error("Failed to send P2P response to %s: %s", addr, exc)


def enter_service_loop(args: RegisterArgs, response: dict, server: DiskServer) -> int:
    logging.info("Registration complete; entering disk command loop")
    print("Available commands: deregister-disk, stats, help")

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
                server.shutdown()
                return 0
            continue

        if command == "stats":
            stats = server.storage.get_storage_stats()
            print(json.dumps(stats, indent=2))
            continue

        if command in {"help", "?"}:
            print("Commands:\n  deregister-disk\n  stats\n  help")
            continue

        if command in {"exit", "quit"}:
            logging.info("Use deregister-disk to cleanly exit the disk client")
            continue

        logging.error("Unknown command '%s'", tokens[0])

    server.shutdown()
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

    # Start the disk server with command port thread
    server = DiskServer(reg_args)
    return enter_service_loop(reg_args, response, server)


if __name__ == "__main__":
    sys.exit(main())
