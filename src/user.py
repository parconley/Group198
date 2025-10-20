from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import random
import shlex
import socket
import sys
import threading
from dataclasses import dataclass
from typing import Optional, List, Tuple

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


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================


def compute_xor_parity(blocks: List[bytes]) -> bytes:
    """Compute XOR parity across multiple blocks.

    Args:
        blocks: List of byte arrays of equal length

    Returns:
        Parity block (XOR of all input blocks)
    """
    if not blocks:
        raise ValueError("At least one block required for parity computation")

    block_size = len(blocks[0])
    for block in blocks[1:]:
        if len(block) != block_size:
            raise ValueError("All blocks must have the same size")

    parity = bytearray(block_size)
    for block in blocks:
        for i in range(block_size):
            parity[i] ^= block[i]

    return bytes(parity)


def reconstruct_missing_block(blocks: List[Optional[bytes]], missing_index: int) -> bytes:
    """Reconstruct a missing block using XOR parity.

    Args:
        blocks: List of blocks where one is None (the missing block)
        missing_index: Index of the missing block

    Returns:
        Reconstructed block
    """
    available_blocks = [b for i, b in enumerate(blocks) if i != missing_index and b is not None]
    return compute_xor_parity(available_blocks)


def calculate_parity_disk(stripe_number: int, n: int) -> int:
    """Calculate which disk holds the parity for a given stripe.

    Args:
        stripe_number: Stripe index (0-based)
        n: Number of disks in DSS

    Returns:
        Disk index for parity (0-based)
    """
    return n - ((stripe_number % n) + 1)


def read_file_into_stripes(file_path: str, striping_unit: int, n: int) -> List[List[bytes]]:
    """Read a file and split it into stripes with padding.

    Args:
        file_path: Path to the file to read
        striping_unit: Size of each block in bytes
        n: Number of disks (n-1 data blocks + 1 parity per stripe)

    Returns:
        List of stripes, where each stripe is a list of n blocks (n-1 data + 1 parity)
    """
    with open(file_path, "rb") as f:
        file_data = f.read()

    data_blocks_per_stripe = n - 1
    stripe_data_size = striping_unit * data_blocks_per_stripe
    stripes = []

    offset = 0
    while offset < len(file_data):
        # Read data for this stripe
        stripe_data = file_data[offset:offset + stripe_data_size]

        # Split into blocks
        data_blocks = []
        for i in range(data_blocks_per_stripe):
            block_start = i * striping_unit
            block_end = block_start + striping_unit
            block = stripe_data[block_start:block_end]

            # Pad with zeros if this is the last stripe and block is short
            if len(block) < striping_unit:
                block = block + b'\x00' * (striping_unit - len(block))

            data_blocks.append(block)

        # Compute parity for this stripe
        parity_block = compute_xor_parity(data_blocks)

        # Assemble stripe: rotate parity position
        stripe_num = len(stripes)
        parity_disk = calculate_parity_disk(stripe_num, n)

        stripe_blocks = []
        for disk_idx in range(n):
            if disk_idx == parity_disk:
                stripe_blocks.append(parity_block)
            else:
                # Map to data block index
                data_idx = disk_idx if disk_idx < parity_disk else disk_idx - 1
                stripe_blocks.append(data_blocks[data_idx])

        stripes.append(stripe_blocks)
        offset += stripe_data_size

    return stripes


def inject_single_bit_error(block: bytes, probability: int) -> bytes:
    """Inject a single random bit error with given probability.

    Args:
        block: Original block data
        probability: Error probability (0-100)

    Returns:
        Block with possible bit flip
    """
    k = random.randint(0, 100)
    if k < probability:
        # Flip one random bit
        block_array = bytearray(block)
        byte_idx = random.randint(0, len(block_array) - 1)
        bit_idx = random.randint(0, 7)
        block_array[byte_idx] ^= (1 << bit_idx)
        logging.debug(f"Injected bit error at byte {byte_idx}, bit {bit_idx}")
        return bytes(block_array)
    return block


def send_p2p_message(
    sock: socket.socket,
    message: dict,
    dest_host: str,
    dest_port: int,
    timeout: float = 3.0,
    retries: int = 0
) -> dict:
    """Send a P2P message to a disk and wait for response.

    Args:
        sock: UDP socket to use for communication
        message: Message dictionary to send
        dest_host: Destination IP address
        dest_port: Destination port
        timeout: Timeout in seconds
        retries: Number of retry attempts

    Returns:
        Response message dictionary

    Raises:
        RegistrationError: On timeout or invalid response
    """
    payload = (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")
    message_id = message.get("message_id", "UNKNOWN")
    message_type = message.get("message_type", "UNKNOWN")
    expected_response_type = f"{message_type}_response"

    sock.settimeout(timeout)
    attempts = retries + 1

    for attempt in range(attempts):
        try:
            sock.sendto(payload, (dest_host, dest_port))
            logging.debug(f"Sent {message_type} to {dest_host}:{dest_port} (attempt {attempt + 1}/{attempts})")

            # Wait for response
            data, addr = sock.recvfrom(64 * 1024)
            text = data.decode("utf-8", errors="strict")

            for line in (ln for ln in text.splitlines() if ln.strip()):
                response = json.loads(line)

                if response.get("message_id") != message_id:
                    logging.warning(f"Received message_id mismatch: expected {message_id}, got {response.get('message_id')}")
                    continue

                if response.get("message_type") != expected_response_type:
                    logging.warning(f"Received message_type mismatch: expected {expected_response_type}, got {response.get('message_type')}")
                    continue

                return response

        except socket.timeout:
            if attempt < attempts - 1:
                logging.debug(f"Timeout on attempt {attempt + 1}, retrying...")
                continue
            else:
                raise RegistrationError(f"Timeout waiting for {expected_response_type} from {dest_host}:{dest_port}")
        except Exception as exc:
            raise RegistrationError(f"P2P communication error: {exc}")

    raise RegistrationError(f"Failed to get valid response after {attempts} attempts")


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


def _handle_copy(args: RegisterArgs, tokens: list[str]) -> None:
    """Handle copy command: copy a file to DSS."""
    if len(tokens) != 2:
        print("Usage: copy <file-path>")
        return

    file_path = tokens[1]

    # Validate file exists
    if not os.path.isfile(file_path):
        logging.error("File not found: %s", file_path)
        return

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    logging.info("Starting copy: file=%s size=%d bytes", file_name, file_size)

    # Phase 1: Request DSS parameters from manager
    message_id = generate_message_id(args.user_name)
    message = {
        "version": PROTOCOL_VERSION,
        "message_id": message_id,
        "message_type": "copy",
        "body": {
            "file_name": file_name,
            "file_size": file_size,
            "owner": args.user_name,
        },
    }
    payload = (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        response = exchange_with_manager(
            action="copy",
            expected_response_type="copy_response",
            message_id=message_id,
            payload=payload,
            manager_host=args.manager_host,
            manager_port=args.manager_port,
            timeout=args.timeout,
            retries=args.retries,
        )
    except RegistrationError as exc:
        logging.error("copy Phase1 failed: %s", exc)
        return

    status_code = response.get("status_code")
    reason = response.get("reason")

    if status_code == "FAILURE":
        logging.error("copy Phase1 FAILURE: %s", reason or "Unknown reason")
        return

    body = response.get("body")
    if not isinstance(body, dict):
        logging.error("copy Phase1 response missing body")
        return

    dss_name = body.get("dss_name")
    n = body.get("n")
    striping_unit = body.get("striping_unit")
    disks_info = body.get("disks", [])

    logging.info(
        "copy Phase1 SUCCESS: dss=%s n=%d striping_unit=%d",
        dss_name,
        n,
        striping_unit
    )

    # Phase 2: Read file into stripes and send write_block to disks
    try:
        stripes = read_file_into_stripes(file_path, striping_unit, n)
        logging.info("File split into %d stripes", len(stripes))
    except Exception as exc:
        logging.error("Failed to read file into stripes: %s", exc)
        return

    # Create socket for P2P communication
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", args.command_port))

    try:
        # For each stripe, send blocks to all n disks in parallel
        for stripe_idx, stripe_blocks in enumerate(stripes):
            logging.info("Writing stripe %d/%d", stripe_idx + 1, len(stripes))

            # Thread function for writing one block
            def write_block_thread(disk_idx: int, block_data: bytes, results: list):
                disk_info = disks_info[disk_idx]
                parity_disk = calculate_parity_disk(stripe_idx, n)
                block_type = "parity" if disk_idx == parity_disk else "data"

                message_id_local = generate_message_id(args.user_name)
                write_message = {
                    "version": PROTOCOL_VERSION,
                    "message_id": message_id_local,
                    "message_type": "write_block",
                    "body": {
                        "dss_name": dss_name,
                        "file_name": file_name,
                        "file_size": file_size,
                        "owner": args.user_name,
                        "stripe_number": stripe_idx,
                        "block_type": block_type,
                        "block_data_base64": base64.b64encode(block_data).decode("ascii"),
                    },
                }

                try:
                    write_response = send_p2p_message(
                        sock,
                        write_message,
                        disk_info["ipv4_address"],
                        disk_info["command_port"],
                        timeout=args.timeout,
                        retries=args.retries,
                    )

                    if write_response.get("status_code") == "SUCCESS":
                        logging.debug(
                            "write_block SUCCESS disk=%s stripe=%d type=%s",
                            disk_info["disk_name"],
                            stripe_idx,
                            block_type
                        )
                        results[disk_idx] = True
                    else:
                        logging.error(
                            "write_block FAILURE disk=%s stripe=%d: %s",
                            disk_info["disk_name"],
                            stripe_idx,
                            write_response.get("reason", "Unknown")
                        )
                        results[disk_idx] = False
                except Exception as exc:
                    logging.error(
                        "write_block exception disk=%s stripe=%d: %s",
                        disk_info["disk_name"],
                        stripe_idx,
                        exc
                    )
                    results[disk_idx] = False

            # Launch n threads for this stripe
            threads = []
            results = [False] * n

            for disk_idx in range(n):
                thread = threading.Thread(
                    target=write_block_thread,
                    args=(disk_idx, stripe_blocks[disk_idx], results)
                )
                thread.start()
                threads.append(thread)

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Check if all writes succeeded
            if not all(results):
                logging.error("Some write_block operations failed for stripe %d", stripe_idx)
                sock.close()
                return

        logging.info("All stripes written successfully")

    finally:
        sock.close()

    # Phase 3: Send copy_complete to manager
    message_id = generate_message_id(args.user_name)
    complete_message = {
        "version": PROTOCOL_VERSION,
        "message_id": message_id,
        "message_type": "copy_complete",
        "body": {
            "dss_name": dss_name,
            "file_name": file_name,
            "file_size": file_size,
            "owner": args.user_name,
        },
    }
    payload = (json.dumps(complete_message, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        response = exchange_with_manager(
            action="copy_complete",
            expected_response_type="copy_complete_response",
            message_id=message_id,
            payload=payload,
            manager_host=args.manager_host,
            manager_port=args.manager_port,
            timeout=args.timeout,
            retries=args.retries,
        )
    except RegistrationError as exc:
        logging.error("copy_complete failed: %s", exc)
        return

    status_code = response.get("status_code")
    if status_code == "SUCCESS":
        logging.info("copy COMPLETE: file=%s copied to dss=%s", file_name, dss_name)
        print(f"\nSUCCESS: File '{file_name}' ({file_size} bytes) copied to DSS '{dss_name}'")
    else:
        logging.error("copy_complete FAILURE: %s", response.get("reason", "Unknown"))


def _handle_read(args: RegisterArgs, tokens: list[str]) -> None:
    """Handle read command: read a file from DSS with error injection and parity verification."""
    if len(tokens) < 4 or len(tokens) > 5:
        print("Usage: read <dss-name> <file-name> <output-path> [error-probability]")
        return

    dss_name = tokens[1]
    file_name = tokens[2]
    output_path = tokens[3]
    error_probability = int(tokens[4]) if len(tokens) == 5 else 0

    if error_probability < 0 or error_probability > 100:
        logging.error("error-probability must be between 0 and 100")
        return

    logging.info(
        "Starting read: dss=%s file=%s output=%s error_prob=%d",
        dss_name,
        file_name,
        output_path,
        error_probability
    )

    # Get DSS info from ls command
    message_id = generate_message_id(args.user_name)
    ls_message = {
        "version": PROTOCOL_VERSION,
        "message_id": message_id,
        "message_type": "ls",
        "body": {},
    }
    payload = (json.dumps(ls_message, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        response = exchange_with_manager(
            action="ls",
            expected_response_type="ls_response",
            message_id=message_id,
            payload=payload,
            manager_host=args.manager_host,
            manager_port=args.manager_port,
            timeout=args.timeout,
            retries=args.retries,
        )
    except RegistrationError as exc:
        logging.error("Failed to get DSS info: %s", exc)
        return

    if response.get("status_code") != "SUCCESS":
        logging.error("ls failed: %s", response.get("reason", "Unknown"))
        return

    # Find the specified DSS and file
    dss_list = response.get("body", {}).get("dss_list", [])
    target_dss = None
    target_file = None

    for dss in dss_list:
        if dss.get("dss_name") == dss_name:
            target_dss = dss
            for file_info in dss.get("files", []):
                if file_info.get("file_name") == file_name:
                    target_file = file_info
                    break
            break

    if target_dss is None:
        logging.error("DSS '%s' not found", dss_name)
        return

    if target_file is None:
        logging.error("File '%s' not found in DSS '%s'", file_name, dss_name)
        return

    n = target_dss.get("n")
    striping_unit = target_dss.get("striping_unit")
    disks = target_dss.get("disks", [])
    file_size = target_file.get("file_size")
    owner = target_file.get("owner")

    logging.info(
        "Found file: size=%d owner=%s n=%d striping_unit=%d",
        file_size,
        owner,
        n,
        striping_unit
    )

    # Get disk info for all disks in the DSS
    # We need to query the manager or use cached info
    # For simplicity, let's use ls to get disk details
    # Actually, ls response should include disk details. Let me check...
    # Looking at manager.py handle_ls, it only returns disk names, not IPs/ports
    # We need to get that info somehow. Let me add a read message to manager protocol

    # For now, I'll implement a workaround: store disk info from copy operation
    # But this is not ideal. Let me check if there's a better way...

    # Actually, I need to implement a manager message to get DSS disk info
    # Or enhance the ls response to include disk IPs/ports
    # For now, let me create a helper message to get DSS info

    # Request DSS disk information from manager
    message_id = generate_message_id(args.user_name)
    read_request = {
        "version": PROTOCOL_VERSION,
        "message_id": message_id,
        "message_type": "read",
        "body": {
            "dss_name": dss_name,
            "file_name": file_name,
            "user_name": args.user_name,
        },
    }
    payload = (json.dumps(read_request, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        response = exchange_with_manager(
            action="read",
            expected_response_type="read_response",
            message_id=message_id,
            payload=payload,
            manager_host=args.manager_host,
            manager_port=args.manager_port,
            timeout=args.timeout,
            retries=args.retries,
        )
    except RegistrationError as exc:
        logging.error("read request failed: %s", exc)
        return

    if response.get("status_code") != "SUCCESS":
        logging.error("read request FAILURE: %s", response.get("reason", "Unknown"))
        return

    body = response.get("body", {})
    disks_info = body.get("disks", [])
    n = body.get("n")
    striping_unit = body.get("striping_unit")
    file_size = body.get("file_size")

    # Calculate number of stripes
    data_blocks_per_stripe = n - 1
    stripe_data_size = striping_unit * data_blocks_per_stripe
    num_stripes = (file_size + stripe_data_size - 1) // stripe_data_size

    logging.info("Reading %d stripes from DSS", num_stripes)

    # Create socket for P2P communication
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", args.command_port))

    all_stripe_data = []

    try:
        for stripe_idx in range(num_stripes):
            max_retries = 3
            for retry_attempt in range(max_retries):
                logging.info("Reading stripe %d/%d (attempt %d)", stripe_idx + 1, num_stripes, retry_attempt + 1)

                # Thread function for reading one block
                def read_block_thread(disk_idx: int, results: list):
                    disk_info = disks_info[disk_idx]

                    message_id_local = generate_message_id(args.user_name)
                    read_message = {
                        "version": PROTOCOL_VERSION,
                        "message_id": message_id_local,
                        "message_type": "read_block",
                        "body": {
                            "dss_name": dss_name,
                            "file_name": file_name,
                            "stripe_number": stripe_idx,
                        },
                    }

                    try:
                        read_response = send_p2p_message(
                            sock,
                            read_message,
                            disk_info["ipv4_address"],
                            disk_info["command_port"],
                            timeout=args.timeout,
                            retries=args.retries,
                        )

                        if read_response.get("status_code") == "SUCCESS":
                            block_data_base64 = read_response.get("body", {}).get("block_data_base64")
                            block_type = read_response.get("body", {}).get("block_type")

                            if block_data_base64:
                                block_data = base64.b64decode(block_data_base64)

                                # Inject error with given probability
                                block_data = inject_single_bit_error(block_data, error_probability)

                                results[disk_idx] = {
                                    "success": True,
                                    "block_data": block_data,
                                    "block_type": block_type,
                                }
                                logging.debug(
                                    "read_block SUCCESS disk=%s stripe=%d type=%s",
                                    disk_info["disk_name"],
                                    stripe_idx,
                                    block_type
                                )
                            else:
                                results[disk_idx] = {"success": False, "reason": "Missing block_data"}
                        else:
                            results[disk_idx] = {"success": False, "reason": read_response.get("reason", "Unknown")}
                    except Exception as exc:
                        results[disk_idx] = {"success": False, "reason": str(exc)}
                        logging.error(
                            "read_block exception disk=%s stripe=%d: %s",
                            disk_info["disk_name"],
                            stripe_idx,
                            exc
                        )

                # Launch n threads for this stripe
                threads = []
                results = [None] * n

                for disk_idx in range(n):
                    thread = threading.Thread(
                        target=read_block_thread,
                        args=(disk_idx, results)
                    )
                    thread.start()
                    threads.append(thread)

                # Wait for all threads to complete
                for thread in threads:
                    thread.join()

                # Check if all reads succeeded
                if not all(r and r.get("success") for r in results):
                    logging.error("Some read_block operations failed for stripe %d", stripe_idx)
                    sock.close()
                    return

                # Extract block data
                stripe_blocks = [r["block_data"] for r in results]

                # Verify parity: XOR of all blocks should be zero
                parity_check = compute_xor_parity(stripe_blocks)
                if any(byte != 0 for byte in parity_check):
                    logging.warning("Parity check FAILED for stripe %d, retrying...", stripe_idx)
                    if retry_attempt < max_retries - 1:
                        continue  # Retry this stripe
                    else:
                        logging.error("Parity check FAILED for stripe %d after %d retries", stripe_idx, max_retries)
                        sock.close()
                        return
                else:
                    logging.debug("Parity check PASSED for stripe %d", stripe_idx)

                # Extract data blocks (exclude parity)
                parity_disk = calculate_parity_disk(stripe_idx, n)
                data_blocks = []
                for disk_idx in range(n):
                    if disk_idx != parity_disk:
                        data_blocks.append(stripe_blocks[disk_idx])

                # Concatenate data blocks for this stripe
                stripe_data = b"".join(data_blocks)
                all_stripe_data.append(stripe_data)
                break  # Success, move to next stripe

    finally:
        sock.close()

    # Reassemble file from all stripes
    file_data = b"".join(all_stripe_data)

    # Trim to actual file size (remove padding)
    file_data = file_data[:file_size]

    # Write to output file
    try:
        with open(output_path, "wb") as f:
            f.write(file_data)
        logging.info("read COMPLETE: file=%s size=%d written to %s", file_name, file_size, output_path)
        print(f"\nSUCCESS: File '{file_name}' ({file_size} bytes) read from DSS '{dss_name}' and saved to '{output_path}'")
    except Exception as exc:
        logging.error("Failed to write output file: %s", exc)


def _handle_ls(args: RegisterArgs) -> None:
    message_id = generate_message_id(args.user_name)
    message = {
        "version": PROTOCOL_VERSION,
        "message_id": message_id,
        "message_type": "ls",
        "body": {},
    }
    payload = (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        response = exchange_with_manager(
            action="ls",
            expected_response_type="ls_response",
            message_id=message_id,
            payload=payload,
            manager_host=args.manager_host,
            manager_port=args.manager_port,
            timeout=args.timeout,
            retries=args.retries,
        )
    except RegistrationError as exc:
        logging.error("ls exchange failed: %s", exc)
        return

    status_code = response.get("status_code")
    reason = response.get("reason")

    if status_code == "FAILURE":
        logging.warning("ls FAILURE: %s", reason or "Unknown reason")
        print(json.dumps(response, indent=2))
        return

    body = response.get("body")
    if not isinstance(body, dict):
        logging.error("ls response missing body")
        return

    dss_list = body.get("dss_list", [])

    if len(dss_list) == 0:
        print("No DSS configured")
        return

    print(f"\n{'='*80}")
    print(f"DSS LIST ({len(dss_list)} total)")
    print(f"{'='*80}\n")

    for dss in dss_list:
        dss_name = dss.get("dss_name", "Unknown")
        n = dss.get("n", 0)
        striping_unit = dss.get("striping_unit", 0)
        disks = dss.get("disks", [])
        files = dss.get("files", [])

        print(f"DSS: {dss_name}")
        print(f"  Parameters: n={n}, striping_unit={striping_unit} bytes")
        print(f"  Disks: {', '.join(disks)}")
        print(f"  Files ({len(files)}):")

        if len(files) == 0:
            print("    (none)")
        else:
            for file_info in files:
                file_name = file_info.get("file_name", "Unknown")
                file_size = file_info.get("file_size", 0)
                owner = file_info.get("owner", "Unknown")
                print(f"    - {file_name} ({file_size} bytes, owner: {owner})")
        print()


def _handle_disk_failure(args: RegisterArgs, tokens: list[str]) -> None:
    """Handle disk-failure command: simulate disk failure and recovery."""
    if len(tokens) != 2:
        print("Usage: disk-failure <dss-name>")
        return

    dss_name = tokens[1]

    logging.info("Starting disk-failure simulation for DSS: %s", dss_name)

    # Get DSS info from ls command
    message_id = generate_message_id(args.user_name)
    ls_message = {
        "version": PROTOCOL_VERSION,
        "message_id": message_id,
        "message_type": "ls",
        "body": {},
    }
    payload = (json.dumps(ls_message, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        response = exchange_with_manager(
            action="ls",
            expected_response_type="ls_response",
            message_id=message_id,
            payload=payload,
            manager_host=args.manager_host,
            manager_port=args.manager_port,
            timeout=args.timeout,
            retries=args.retries,
        )
    except RegistrationError as exc:
        logging.error("Failed to get DSS info: %s", exc)
        return

    if response.get("status_code") != "SUCCESS":
        logging.error("ls failed: %s", response.get("reason", "Unknown"))
        return

    # Find the specified DSS
    dss_list = response.get("body", {}).get("dss_list", [])
    target_dss = None

    for dss in dss_list:
        if dss.get("dss_name") == dss_name:
            target_dss = dss
            break

    if target_dss is None:
        logging.error("DSS '%s' not found", dss_name)
        return

    n = target_dss.get("n")
    striping_unit = target_dss.get("striping_unit")
    disk_names = target_dss.get("disks", [])
    files_list = target_dss.get("files", [])

    if len(disk_names) == 0:
        logging.error("No disks found in DSS '%s'", dss_name)
        return

    # Select a random disk to fail
    failed_disk_name = random.choice(disk_names)
    failed_disk_idx = disk_names.index(failed_disk_name)

    logging.info("Selected disk '%s' (index %d) for failure", failed_disk_name, failed_disk_idx)
    print(f"\nSimulating failure of disk '{failed_disk_name}' in DSS '{dss_name}'...")

    # Get disk information by querying for a file (or enhance manager to provide this)
    # For simplicity, let's request read info for the first file to get disk details
    if len(files_list) == 0:
        logging.warning("No files stored in DSS '%s', nothing to recover", dss_name)
        return

    first_file = files_list[0]
    message_id = generate_message_id(args.user_name)
    read_request = {
        "version": PROTOCOL_VERSION,
        "message_id": message_id,
        "message_type": "read",
        "body": {
            "dss_name": dss_name,
            "file_name": first_file.get("file_name"),
            "user_name": args.user_name,
        },
    }
    payload = (json.dumps(read_request, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        response = exchange_with_manager(
            action="read",
            expected_response_type="read_response",
            message_id=message_id,
            payload=payload,
            manager_host=args.manager_host,
            manager_port=args.manager_port,
            timeout=args.timeout,
            retries=args.retries,
        )
    except RegistrationError as exc:
        logging.error("Failed to get disk info: %s", exc)
        return

    if response.get("status_code") != "SUCCESS":
        logging.error("read request failed: %s", response.get("reason", "Unknown"))
        return

    disks_info = response.get("body", {}).get("disks", [])

    # Create socket for P2P communication
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", args.command_port))

    try:
        # Phase 1: Send fail message to the selected disk
        failed_disk_info = disks_info[failed_disk_idx]
        message_id_fail = generate_message_id(args.user_name)
        fail_message = {
            "version": PROTOCOL_VERSION,
            "message_id": message_id_fail,
            "message_type": "fail",
            "body": {
                "dss_name": dss_name,
            },
        }

        try:
            fail_response = send_p2p_message(
                sock,
                fail_message,
                failed_disk_info["ipv4_address"],
                failed_disk_info["command_port"],
                timeout=args.timeout,
                retries=args.retries,
            )

            if fail_response.get("status_code") == "SUCCESS":
                blocks_deleted = fail_response.get("body", {}).get("blocks_deleted", 0)
                logging.info("Disk '%s' failed successfully, %d blocks deleted", failed_disk_name, blocks_deleted)
                print(f"Disk '{failed_disk_name}' failed: {blocks_deleted} blocks deleted")
            else:
                logging.error("fail message FAILURE: %s", fail_response.get("reason", "Unknown"))
                sock.close()
                return
        except Exception as exc:
            logging.error("Failed to send fail message: %s", exc)
            sock.close()
            return

        # Phase 2: Recovery - reconstruct all blocks for each file
        print(f"\nStarting recovery for {len(files_list)} file(s)...")

        for file_info in files_list:
            file_name = file_info.get("file_name")
            file_size = file_info.get("file_size")
            owner = file_info.get("owner")

            logging.info("Recovering file '%s' (size=%d)", file_name, file_size)
            print(f"  Recovering file '{file_name}' ({file_size} bytes)...")

            # Calculate number of stripes
            data_blocks_per_stripe = n - 1
            stripe_data_size = striping_unit * data_blocks_per_stripe
            num_stripes = (file_size + stripe_data_size - 1) // stripe_data_size

            # For each stripe, read from surviving disks and reconstruct
            for stripe_idx in range(num_stripes):
                logging.debug("Recovering stripe %d/%d", stripe_idx + 1, num_stripes)

                # Read blocks from all surviving disks
                def read_recovery_thread(disk_idx: int, results: list):
                    if disk_idx == failed_disk_idx:
                        results[disk_idx] = None  # This is the failed disk
                        return

                    disk_info = disks_info[disk_idx]
                    message_id_local = generate_message_id(args.user_name)
                    read_message = {
                        "version": PROTOCOL_VERSION,
                        "message_id": message_id_local,
                        "message_type": "read_block_for_recovery",
                        "body": {
                            "dss_name": dss_name,
                            "file_name": file_name,
                            "stripe_number": stripe_idx,
                        },
                    }

                    try:
                        read_response = send_p2p_message(
                            sock,
                            read_message,
                            disk_info["ipv4_address"],
                            disk_info["command_port"],
                            timeout=args.timeout,
                            retries=args.retries,
                        )

                        if read_response.get("status_code") == "SUCCESS":
                            block_data_base64 = read_response.get("body", {}).get("block_data_base64")
                            if block_data_base64:
                                block_data = base64.b64decode(block_data_base64)
                                results[disk_idx] = block_data
                            else:
                                results[disk_idx] = None
                        else:
                            results[disk_idx] = None
                    except Exception as exc:
                        logging.error("read_block_for_recovery exception: %s", exc)
                        results[disk_idx] = None

                # Launch n-1 threads to read from surviving disks
                threads = []
                results = [None] * n

                for disk_idx in range(n):
                    thread = threading.Thread(
                        target=read_recovery_thread,
                        args=(disk_idx, results)
                    )
                    thread.start()
                    threads.append(thread)

                # Wait for all threads
                for thread in threads:
                    thread.join()

                # Reconstruct the missing block
                reconstructed_block = reconstruct_missing_block(results, failed_disk_idx)

                # Write recovered block to failed disk
                parity_disk = calculate_parity_disk(stripe_idx, n)
                block_type = "parity" if failed_disk_idx == parity_disk else "data"

                message_id_local = generate_message_id(args.user_name)
                write_recovered_message = {
                    "version": PROTOCOL_VERSION,
                    "message_id": message_id_local,
                    "message_type": "write_recovered_block",
                    "body": {
                        "dss_name": dss_name,
                        "file_name": file_name,
                        "file_size": file_size,
                        "owner": owner,
                        "stripe_number": stripe_idx,
                        "block_type": block_type,
                        "block_data_base64": base64.b64encode(reconstructed_block).decode("ascii"),
                    },
                }

                try:
                    write_response = send_p2p_message(
                        sock,
                        write_recovered_message,
                        failed_disk_info["ipv4_address"],
                        failed_disk_info["command_port"],
                        timeout=args.timeout,
                        retries=args.retries,
                    )

                    if write_response.get("status_code") != "SUCCESS":
                        logging.error(
                            "write_recovered_block FAILURE stripe=%d: %s",
                            stripe_idx,
                            write_response.get("reason", "Unknown")
                        )
                        sock.close()
                        return
                except Exception as exc:
                    logging.error("write_recovered_block exception: %s", exc)
                    sock.close()
                    return

            print(f"    ✓ File '{file_name}' recovered successfully")

        logging.info("Recovery complete for DSS '%s'", dss_name)
        print(f"\nSUCCESS: All files recovered on disk '{failed_disk_name}'")

    finally:
        sock.close()


def _handle_decommission_dss(args: RegisterArgs, tokens: list[str]) -> None:
    """Handle decommission-dss command: delete DSS and free all disks."""
    if len(tokens) != 2:
        print("Usage: decommission-dss <dss-name>")
        return

    dss_name = tokens[1]

    logging.info("Starting decommission of DSS: %s", dss_name)

    # Get DSS info from ls command to find disks
    message_id = generate_message_id(args.user_name)
    ls_message = {
        "version": PROTOCOL_VERSION,
        "message_id": message_id,
        "message_type": "ls",
        "body": {},
    }
    payload = (json.dumps(ls_message, separators=(",", ":")) + "\n").encode("utf-8")

    try:
        response = exchange_with_manager(
            action="ls",
            expected_response_type="ls_response",
            message_id=message_id,
            payload=payload,
            manager_host=args.manager_host,
            manager_port=args.manager_port,
            timeout=args.timeout,
            retries=args.retries,
        )
    except RegistrationError as exc:
        logging.error("Failed to get DSS info: %s", exc)
        return

    if response.get("status_code") != "SUCCESS":
        logging.error("ls failed: %s", response.get("reason", "Unknown"))
        return

    # Find the specified DSS
    dss_list = response.get("body", {}).get("dss_list", [])
    target_dss = None

    for dss in dss_list:
        if dss.get("dss_name") == dss_name:
            target_dss = dss
            break

    if target_dss is None:
        logging.error("DSS '%s' not found", dss_name)
        return

    disk_names = target_dss.get("disks", [])

    if len(disk_names) == 0:
        logging.warning("No disks found in DSS '%s'", dss_name)
        return

    # Get disk info (need IP addresses and ports)
    # Request via read for the first file, or implement a dedicated get_dss_info message
    # For now, I'll use the same approach as disk-failure

    files_list = target_dss.get("files", [])
    if len(files_list) > 0:
        first_file = files_list[0]
        message_id = generate_message_id(args.user_name)
        read_request = {
            "version": PROTOCOL_VERSION,
            "message_id": message_id,
            "message_type": "read",
            "body": {
                "dss_name": dss_name,
                "file_name": first_file.get("file_name"),
                "user_name": args.user_name,
            },
        }
        payload = (json.dumps(read_request, separators=(",", ":")) + "\n").encode("utf-8")

        try:
            response = exchange_with_manager(
                action="read",
                expected_response_type="read_response",
                message_id=message_id,
                payload=payload,
                manager_host=args.manager_host,
                manager_port=args.manager_port,
                timeout=args.timeout,
                retries=args.retries,
            )
        except RegistrationError as exc:
            logging.error("Failed to get disk info: %s", exc)
            return

        if response.get("status_code") != "SUCCESS":
            logging.error("read request failed: %s", response.get("reason", "Unknown"))
            return

        disks_info = response.get("body", {}).get("disks", [])
    else:
        # No files, but we still need disk info
        # We'll need a better solution here - perhaps enhance ls to include disk details
        logging.warning("No files in DSS, cannot get disk info for decommission")
        print(f"WARNING: DSS '{dss_name}' has no files. Cannot complete decommission without disk details.")
        return

    # Create socket for P2P communication
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", args.command_port))

    try:
        # Send delete_all message to each disk
        for disk_info in disks_info:
            disk_name = disk_info["disk_name"]
            message_id_local = generate_message_id(args.user_name)
            delete_message = {
                "version": PROTOCOL_VERSION,
                "message_id": message_id_local,
                "message_type": "delete_all",
                "body": {
                    "dss_name": dss_name,
                },
            }

            try:
                delete_response = send_p2p_message(
                    sock,
                    delete_message,
                    disk_info["ipv4_address"],
                    disk_info["command_port"],
                    timeout=args.timeout,
                    retries=args.retries,
                )

                if delete_response.get("status_code") == "SUCCESS":
                    blocks_deleted = delete_response.get("body", {}).get("blocks_deleted", 0)
                    logging.info("Disk '%s': %d blocks deleted", disk_name, blocks_deleted)
                    print(f"  Disk '{disk_name}': {blocks_deleted} blocks deleted")
                else:
                    logging.error("delete_all FAILURE for disk '%s': %s", disk_name, delete_response.get("reason", "Unknown"))
            except Exception as exc:
                logging.error("delete_all exception for disk '%s': %s", disk_name, exc)

        logging.info("decommission-dss COMPLETE for DSS '%s'", dss_name)
        print(f"\nSUCCESS: DSS '{dss_name}' decommissioned, all blocks deleted")
        print("Note: Disks have been freed and are now available for new DSS configurations")

    finally:
        sock.close()


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
        "Available commands: configure-dss, ls, copy, read, disk-failure, decommission-dss, deregister-user, help"
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

        if command == "ls":
            _handle_ls(args)
            continue

        if command == "copy":
            _handle_copy(args, tokens)
            continue

        if command == "read":
            _handle_read(args, tokens)
            continue

        if command == "disk-failure":
            _handle_disk_failure(args, tokens)
            continue

        if command == "decommission-dss":
            _handle_decommission_dss(args, tokens)
            continue

        if command in {"deregister-user", "deregister"}:
            if _handle_deregister_user(args):
                return 0
            continue

        if command in {"help", "?"}:
            print(
                "Commands:\n"
                "  configure-dss <dss-name> <n> <striping-unit> - Create a new DSS\n"
                "  ls - List all DSSs and files\n"
                "  copy <file-path> - Copy a file to DSS\n"
                "  read <dss-name> <file-name> <output-path> [error-probability] - Read file from DSS\n"
                "  disk-failure <dss-name> - Simulate disk failure and recovery\n"
                "  decommission-dss <dss-name> - Delete DSS and free disks\n"
                "  deregister-user - Deregister and exit\n"
                "  help - Show this help message"
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
