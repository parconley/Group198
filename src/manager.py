from __future__ import annotations

import argparse
import ipaddress
import json
import logging
import random
import signal
import socket
import sys
import re
from dataclasses import dataclass, replace
from typing import Any, Dict, Optional, Tuple

PROTOCOL_VERSION = 1
VALID_PORT_MIN = 21200
VALID_PORT_MAX = 21299
MIN_STRIPING_UNIT = 128
MAX_STRIPING_UNIT = 1024 * 1024
MESSAGE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+-\d+-\d+-\d{5,}$")


@dataclass(frozen=True)
class UserRecord:
    user_name: str
    ipv4_address: str
    management_port: int
    command_port: int


@dataclass(frozen=True)
class DiskRecord:
    disk_name: str
    ipv4_address: str
    management_port: int
    command_port: int
    state: str = "Free"
    member_of: Optional[str] = None


@dataclass(frozen=True)
class DssRecord:
    dss_name: str
    n: int
    striping_unit: int
    disks: Tuple[str, ...]
    owner_user: str


@dataclass(frozen=True)
class FileRecord:
    file_name: str
    file_size: int
    owner: str


def _is_power_of_two(value: int) -> bool:
    return value > 0 and (value & (value - 1)) == 0


class ManagerServer:
    """UDP server for DSS manager."""

    def __init__(self, listen_port: int):
        self.listen_port: int = listen_port
        # user_name -> record
        self.users: Dict[str, UserRecord] = {}
        # disk_name -> record
        self.disks: Dict[str, DiskRecord] = {}
        # dss_name -> record
        self.dss_catalog: Dict[str, DssRecord] = {}
        # disk_name -> dss_name
        self.disk_membership: Dict[str, str] = {}
        # port -> owner identifier (e.g., "user:Alice" or "disk:DiskA")
        self.claimed_ports: Dict[int, str] = {}
        # (dss_name, file_name) -> FileRecord
        self.files: Dict[Tuple[str, str], FileRecord] = {}
        # Critical section tracking for copy/read operations
        self.in_progress_copy: Optional[str] = None  # dss_name currently being copied to
        self.in_progress_reads: set = set()  # Set of (dss_name, file_name) tuples

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
            # Cannot mirror message_id - send minimal failure with synthetic id
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

        if not isinstance(message_type, str):
            logging.info("Missing or invalid message_type from %s", addr)
            self._send_response(
                addr,
                message_type="register_user_response",
                message_id=message_id,
                status_code="FAILURE",
                reason="message_type must be a string",
            )
            return

        if message_type == "register_user":
            self.handle_register_user(message_id, body, addr)
            return

        if message_type == "register_disk":
            self.handle_register_disk(message_id, body, addr)
            return

        if message_type == "configure_dss":
            self.handle_configure_dss(message_id, body, addr)
            return

        if message_type == "ls":
            self.handle_ls(message_id, body, addr)
            return

        if message_type == "copy":
            self.handle_copy(message_id, body, addr)
            return

        if message_type == "copy_complete":
            self.handle_copy_complete(message_id, body, addr)
            return

        if message_type == "read":
            self.handle_read(message_id, body, addr)
            return

        if message_type == "deregister_user":
            self.handle_deregister_user(message_id, body, addr)
            return

        if message_type == "deregister_disk":
            self.handle_deregister_disk(message_id, body, addr)
            return

        logging.info("Unknown message_type '%s' from %s", message_type, addr)
        self._send_response(
            addr,
            message_type=f"{message_type}_response" if isinstance(message_type, str) else "register_user_response",
            message_id=message_id,
            status_code="FAILURE",
            reason="Unsupported message_type",
        )

    def handle_register_user(self, message_id: str, body: Any, addr: Tuple[str, int]) -> None:
        """Validate and register a user. Sends response to addr."""
        # Validate base fields
        error = self._validate_register_user_body(body)
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

    def handle_register_disk(self, message_id: str, body: Any, addr: Tuple[str, int]) -> None:
        """Validate and register a disk. Sends response to addr."""
        error = self._validate_register_disk_body(body)
        if error is not None:
            logging.info("register_disk FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="register_disk_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        assert isinstance(body, dict)
        disk_name = body["disk_name"]
        ipv4_address = body["ipv4_address"]
        management_port = int(body["management_port"])
        command_port = int(body["command_port"])

        if disk_name in self.disks:
            error = f"disk_name '{disk_name}' is already registered"
            logging.info("register_disk FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="register_disk_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        if management_port == command_port:
            error = "management_port and command_port must be different"
            logging.info("register_disk FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="register_disk_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        for port in (management_port, command_port):
            owner = self.claimed_ports.get(port)
            if owner is not None:
                error = f"port {port} is already claimed by {owner}"
                logging.info("register_disk FAILURE from %s: %s", addr, error)
                self._send_response(
                    addr,
                    message_type="register_disk_response",
                    message_id=message_id,
                    status_code="FAILURE",
                    reason=error,
                )
                return

        record = DiskRecord(
            disk_name=disk_name,
            ipv4_address=ipv4_address,
            management_port=management_port,
            command_port=command_port,
        )
        self.disks[disk_name] = record
        self.claimed_ports[management_port] = f"disk:{disk_name}"
        self.claimed_ports[command_port] = f"disk:{disk_name}"

        logging.info(
            "register_disk SUCCESS disk=%s ip=%s mgmt=%d cmd=%d from %s",
            disk_name,
            ipv4_address,
            management_port,
            command_port,
            addr,
        )

        self._send_response(
            addr,
            message_type="register_disk_response",
            message_id=message_id,
            status_code="SUCCESS",
            reason=None,
            body={"state": record.state},
        )

    def handle_configure_dss(self, message_id: str, body: Any, addr: Tuple[str, int]) -> None:
        """Configure a new DSS by selecting free disks."""
        error = self._validate_configure_dss_body(body)
        if error is not None:
            logging.info("configure_dss FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="configure_dss_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        assert isinstance(body, dict)
        user_name = body["user_name"]
        dss_name = body["dss_name"]
        n = int(body["n"])
        striping_unit = int(body["striping_unit"])

        if user_name not in self.users:
            error = f"user_name '{user_name}' is not registered"
            logging.info("configure_dss FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="configure_dss_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        if dss_name in self.dss_catalog:
            error = f"dss_name '{dss_name}' already exists"
            logging.info("configure_dss FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="configure_dss_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        free_disks = [
            disk_name
            for disk_name, record in self.disks.items()
            if record.state == "Free" and record.member_of is None
        ]
        if len(free_disks) < n:
            error = f"insufficient free disks: required {n}, available {len(free_disks)}"
            logging.info("configure_dss FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="configure_dss_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        selected_disks = tuple(random.sample(free_disks, n))

        for disk_name in selected_disks:
            record = self.disks[disk_name]
            updated = replace(record, state="InDSS", member_of=dss_name)
            self.disks[disk_name] = updated
            self.disk_membership[disk_name] = dss_name

        dss_record = DssRecord(
            dss_name=dss_name,
            n=n,
            striping_unit=striping_unit,
            disks=selected_disks,
            owner_user=user_name,
        )
        self.dss_catalog[dss_name] = dss_record

        logging.info(
            "configure_dss SUCCESS dss=%s owner=%s n=%d striping=%d disks=%s from %s",
            dss_name,
            user_name,
            n,
            striping_unit,
            ",".join(selected_disks),
            addr,
        )

        self._send_response(
            addr,
            message_type="configure_dss_response",
            message_id=message_id,
            status_code="SUCCESS",
            reason=None,
            body={
                "dss_name": dss_name,
                "n": n,
                "striping_unit": striping_unit,
                "disks": list(selected_disks),
            },
        )

    def handle_ls(self, message_id: str, body: Any, addr: Tuple[str, int]) -> None:
        """List all files across all DSSs."""
        if len(self.dss_catalog) == 0:
            logging.info("ls FAILURE from %s: no DSS configured", addr)
            self._send_response(
                addr,
                message_type="ls_response",
                message_id=message_id,
                status_code="FAILURE",
                reason="No DSS is configured"
            )
            return

        # Build response with DSS info and files
        dss_list = []
        for dss_name, dss_record in self.dss_catalog.items():
            # Get files for this DSS
            files_list = []
            for (file_dss, file_name), file_record in self.files.items():
                if file_dss == dss_name:
                    files_list.append({
                        "file_name": file_record.file_name,
                        "file_size": file_record.file_size,
                        "owner": file_record.owner
                    })

            dss_list.append({
                "dss_name": dss_record.dss_name,
                "n": dss_record.n,
                "disks": list(dss_record.disks),
                "striping_unit": dss_record.striping_unit,
                "files": files_list
            })

        logging.info("ls SUCCESS from %s: %d DSSs", addr, len(dss_list))
        self._send_response(
            addr,
            message_type="ls_response",
            message_id=message_id,
            status_code="SUCCESS",
            reason=None,
            body={"dss_list": dss_list}
        )

    def handle_copy(self, message_id: str, body: Any, addr: Tuple[str, int]) -> None:
        """Phase 1 of copy: select DSS and send parameters to user."""
        # Check if in critical section
        if self.in_progress_copy is not None:
            logging.info("copy FAILURE from %s: copy already in progress for DSS %s", addr, self.in_progress_copy)
            self._send_response(
                addr,
                message_type="copy_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=f"Copy operation already in progress for DSS '{self.in_progress_copy}'"
            )
            return

        if not isinstance(body, dict):
            self._send_response(
                addr,
                message_type="copy_response",
                message_id=message_id,
                status_code="FAILURE",
                reason="body must be an object"
            )
            return

        required = ["file_name", "file_size", "owner"]
        for key in required:
            if key not in body:
                self._send_response(
                    addr,
                    message_type="copy_response",
                    message_id=message_id,
                    status_code="FAILURE",
                    reason=f"missing field: {key}"
                )
                return

        file_name = body["file_name"]
        file_size = int(body["file_size"])
        owner = body["owner"]

        # Verify owner is registered
        if owner not in self.users:
            logging.info("copy FAILURE from %s: owner '%s' not registered", addr, owner)
            self._send_response(
                addr,
                message_type="copy_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=f"owner '{owner}' is not registered"
            )
            return

        if len(self.dss_catalog) == 0:
            logging.info("copy FAILURE from %s: no DSS configured", addr)
            self._send_response(
                addr,
                message_type="copy_response",
                message_id=message_id,
                status_code="FAILURE",
                reason="No DSS exists"
            )
            return

        # Select first available DSS (could be more sophisticated)
        dss_name = list(self.dss_catalog.keys())[0]
        dss_record = self.dss_catalog[dss_name]

        # Enter critical section
        self.in_progress_copy = dss_name

        # Build disk info list
        disks_info = []
        for disk_name in dss_record.disks:
            disk_record = self.disks[disk_name]
            disks_info.append({
                "disk_name": disk_name,
                "ipv4_address": disk_record.ipv4_address,
                "command_port": disk_record.command_port
            })

        logging.info(
            "copy Phase1 SUCCESS from %s: file=%s size=%d owner=%s dss=%s",
            addr,
            file_name,
            file_size,
            owner,
            dss_name
        )

        self._send_response(
            addr,
            message_type="copy_response",
            message_id=message_id,
            status_code="SUCCESS",
            reason=None,
            body={
                "dss_name": dss_name,
                "n": dss_record.n,
                "striping_unit": dss_record.striping_unit,
                "disks": disks_info
            }
        )

    def handle_read(self, message_id: str, body: Any, addr: Tuple[str, int]) -> None:
        """Handle read request: provide DSS and file information for reading."""
        if not isinstance(body, dict):
            self._send_response(
                addr,
                message_type="read_response",
                message_id=message_id,
                status_code="FAILURE",
                reason="body must be an object"
            )
            return

        required = ["dss_name", "file_name", "user_name"]
        for key in required:
            if key not in body:
                self._send_response(
                    addr,
                    message_type="read_response",
                    message_id=message_id,
                    status_code="FAILURE",
                    reason=f"missing field: {key}"
                )
                return

        dss_name = body["dss_name"]
        file_name = body["file_name"]
        user_name = body["user_name"]

        # Verify DSS exists
        if dss_name not in self.dss_catalog:
            logging.info("read FAILURE from %s: DSS '%s' not found", addr, dss_name)
            self._send_response(
                addr,
                message_type="read_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=f"DSS '{dss_name}' does not exist"
            )
            return

        # Verify file exists
        file_key = (dss_name, file_name)
        if file_key not in self.files:
            logging.info("read FAILURE from %s: file '%s' not found in DSS '%s'", addr, file_name, dss_name)
            self._send_response(
                addr,
                message_type="read_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=f"File '{file_name}' not found in DSS '{dss_name}'"
            )
            return

        dss_record = self.dss_catalog[dss_name]
        file_record = self.files[file_key]

        # CRITICAL: Verify ownership - user can only read their own files
        if file_record.owner != user_name:
            logging.info(
                "read FAILURE from %s: user '%s' cannot read file '%s' owned by '%s'",
                addr,
                user_name,
                file_name,
                file_record.owner
            )
            self._send_response(
                addr,
                message_type="read_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=f"Permission denied: file '{file_name}' is owned by '{file_record.owner}', not '{user_name}'"
            )
            return

        # Build disk info list
        disks_info = []
        for disk_name in dss_record.disks:
            disk_record = self.disks[disk_name]
            disks_info.append({
                "disk_name": disk_name,
                "ipv4_address": disk_record.ipv4_address,
                "command_port": disk_record.command_port
            })

        logging.info(
            "read SUCCESS from %s: dss=%s file=%s size=%d owner=%s",
            addr,
            dss_name,
            file_name,
            file_record.file_size,
            file_record.owner
        )

        self._send_response(
            addr,
            message_type="read_response",
            message_id=message_id,
            status_code="SUCCESS",
            reason=None,
            body={
                "dss_name": dss_name,
                "file_name": file_name,
                "file_size": file_record.file_size,
                "owner": file_record.owner,
                "n": dss_record.n,
                "striping_unit": dss_record.striping_unit,
                "disks": disks_info
            }
        )

    def handle_copy_complete(self, message_id: str, body: Any, addr: Tuple[str, int]) -> None:
        """Phase 2 of copy: user confirms completion, manager updates directory."""
        if not isinstance(body, dict):
            self._send_response(
                addr,
                message_type="copy_complete_response",
                message_id=message_id,
                status_code="FAILURE",
                reason="body must be an object"
            )
            return

        required = ["dss_name", "file_name", "file_size", "owner"]
        for key in required:
            if key not in body:
                self._send_response(
                    addr,
                    message_type="copy_complete_response",
                    message_id=message_id,
                    status_code="FAILURE",
                    reason=f"missing field: {key}"
                )
                return

        dss_name = body["dss_name"]
        file_name = body["file_name"]
        file_size = int(body["file_size"])
        owner = body["owner"]

        # Verify we're in the right critical section
        if self.in_progress_copy != dss_name:
            logging.warning(
                "copy_complete FAILURE from %s: expected DSS '%s' but got '%s'",
                addr,
                self.in_progress_copy,
                dss_name
            )
            self._send_response(
                addr,
                message_type="copy_complete_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=f"No copy in progress for DSS '{dss_name}'"
            )
            return

        # Add file to directory
        file_record = FileRecord(
            file_name=file_name,
            file_size=file_size,
            owner=owner
        )
        self.files[(dss_name, file_name)] = file_record

        # Exit critical section
        self.in_progress_copy = None

        logging.info(
            "copy_complete SUCCESS from %s: dss=%s file=%s size=%d owner=%s",
            addr,
            dss_name,
            file_name,
            file_size,
            owner
        )

        self._send_response(
            addr,
            message_type="copy_complete_response",
            message_id=message_id,
            status_code="SUCCESS",
            reason=None
        )

    def handle_deregister_user(self, message_id: str, body: Any, addr: Tuple[str, int]) -> None:
        error = self._validate_deregister_user_body(body)
        if error is not None:
            logging.info("deregister_user FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="deregister_user_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        assert isinstance(body, dict)
        user_name = body["user_name"]
        record = self.users.get(user_name)
        if record is None:
            error = f"user_name '{user_name}' is not registered"
            logging.info("deregister_user FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="deregister_user_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        self.users.pop(user_name, None)
        self.claimed_ports.pop(record.management_port, None)
        self.claimed_ports.pop(record.command_port, None)

        logging.info(
            "deregister_user SUCCESS user=%s mgmt=%d cmd=%d from %s",
            user_name,
            record.management_port,
            record.command_port,
            addr,
        )

        self._send_response(
            addr,
            message_type="deregister_user_response",
            message_id=message_id,
            status_code="SUCCESS",
            reason=None,
        )

    def handle_deregister_disk(self, message_id: str, body: Any, addr: Tuple[str, int]) -> None:
        error = self._validate_deregister_disk_body(body)
        if error is not None:
            logging.info("deregister_disk FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="deregister_disk_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        assert isinstance(body, dict)
        disk_name = body["disk_name"]
        record = self.disks.get(disk_name)
        if record is None:
            error = f"disk_name '{disk_name}' is not registered"
            logging.info("deregister_disk FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="deregister_disk_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        if record.state != "Free" or record.member_of is not None:
            error = f"disk '{disk_name}' is currently part of DSS '{record.member_of}'"
            logging.info("deregister_disk FAILURE from %s: %s", addr, error)
            self._send_response(
                addr,
                message_type="deregister_disk_response",
                message_id=message_id,
                status_code="FAILURE",
                reason=error,
            )
            return

        self.disks.pop(disk_name, None)
        self.claimed_ports.pop(record.management_port, None)
        self.claimed_ports.pop(record.command_port, None)
        self.disk_membership.pop(disk_name, None)

        logging.info(
            "deregister_disk SUCCESS disk=%s mgmt=%d cmd=%d from %s",
            disk_name,
            record.management_port,
            record.command_port,
            addr,
        )

        self._send_response(
            addr,
            message_type="deregister_disk_response",
            message_id=message_id,
            status_code="SUCCESS",
            reason=None,
        )

    def _validate_register_user_body(self, body: Any) -> Optional[str]:
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

    def _validate_register_disk_body(self, body: Any) -> Optional[str]:
        if not isinstance(body, dict):
            return "body must be an object"

        required = ["disk_name", "ipv4_address", "management_port", "command_port"]
        for key in required:
            if key not in body:
                return f"missing field: {key}"

        disk_name = body["disk_name"]
        if not isinstance(disk_name, str):
            return "disk_name must be a string"
        if len(disk_name) == 0 or len(disk_name) > 15:
            return "disk_name length must be 1..15"
        if not disk_name.isalpha():
            return "disk_name must contain only alphabetic characters"

        ipv4 = body["ipv4_address"]
        if not isinstance(ipv4, str):
            return "ipv4_address must be a string"
        try:
            ipaddress.IPv4Address(ipv4)
        except Exception:
            return "ipv4_address must be a valid IPv4 address"

        try:
            mgmt_port = int(body["management_port"])
            cmd_port = int(body["command_port"])
        except Exception:
            return "management_port and command_port must be integers"

        for label, port in ("management_port", mgmt_port), ("command_port", cmd_port):
            if port < VALID_PORT_MIN or port > VALID_PORT_MAX:
                return f"{label} must be in range {VALID_PORT_MIN}-{VALID_PORT_MAX}"

        return None

    def _validate_configure_dss_body(self, body: Any) -> Optional[str]:
        if not isinstance(body, dict):
            return "body must be an object"

        required = ["user_name", "dss_name", "n", "striping_unit"]
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

        dss_name = body["dss_name"]
        if not isinstance(dss_name, str):
            return "dss_name must be a string"
        if len(dss_name) == 0 or len(dss_name) > 15:
            return "dss_name length must be 1..15"
        if not dss_name.isalpha():
            return "dss_name must contain only alphabetic characters"

        try:
            n_value = int(body["n"])
        except Exception:
            return "n must be an integer"
        if n_value < 3:
            return "n must be at least 3"

        try:
            striping_unit = int(body["striping_unit"])
        except Exception:
            return "striping_unit must be an integer"
        if striping_unit < MIN_STRIPING_UNIT or striping_unit > MAX_STRIPING_UNIT:
            return (
                f"striping_unit must be between {MIN_STRIPING_UNIT} and {MAX_STRIPING_UNIT} bytes"
            )
        if not _is_power_of_two(striping_unit):
            return "striping_unit must be a power of two"

        return None

    def _validate_deregister_user_body(self, body: Any) -> Optional[str]:
        if not isinstance(body, dict):
            return "body must be an object"

        if "user_name" not in body:
            return "missing field: user_name"

        user_name = body["user_name"]
        if not isinstance(user_name, str):
            return "user_name must be a string"
        if len(user_name) == 0 or len(user_name) > 15:
            return "user_name length must be 1..15"
        if not user_name.isalpha():
            return "user_name must contain only alphabetic characters"

        return None

    def _validate_deregister_disk_body(self, body: Any) -> Optional[str]:
        if not isinstance(body, dict):
            return "body must be an object"

        if "disk_name" not in body:
            return "missing field: disk_name"

        disk_name = body["disk_name"]
        if not isinstance(disk_name, str):
            return "disk_name must be a string"
        if len(disk_name) == 0 or len(disk_name) > 15:
            return "disk_name length must be 1..15"
        if not disk_name.isalpha():
            return "disk_name must contain only alphabetic characters"

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


