# P2P Protocol Specification for DSS

This document defines the peer-to-peer message protocols for communication between users and disks in the Distributed Storage System.

## Overview

All P2P messages follow the same JSON format as manager messages:

```json
{
  "version": 1,
  "message_id": "<role>-<pid>-<timestamp_ms>-<counter>",
  "message_type": "<message_type>",
  "body": { ... },
  "status_code": "SUCCESS|FAILURE",  // only in responses
  "reason": "..."  // only in failure responses
}
```

## 1. Copy Operation (User → Disk)

### 1.1 write_block (User → Disk)

Sent by user to write a single block (data or parity) to a disk during the copy operation.

**Request:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "write_block",
  "body": {
    "dss_name": "DemoDSS",
    "file_name": "tale-of-two-cities.txt",
    "file_size": 760569,
    "owner": "Alice",
    "stripe_number": 0,
    "block_type": "data|parity",
    "block_data_base64": "<base64-encoded-block>"
  }
}
```

**Fields:**
- `dss_name`: Name of the DSS this file belongs to
- `file_name`: Name of the file being copied
- `file_size`: Total size of the file in bytes
- `owner`: User who owns the file
- `stripe_number`: Stripe index (0-based)
- `block_type`: Either "data" or "parity"
- `block_data_base64`: Base64-encoded block data (exactly striping_unit bytes before encoding)

**Response:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "write_block_response",
  "status_code": "SUCCESS",
  "reason": null
}
```

**Failure Response:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "write_block_response",
  "status_code": "FAILURE",
  "reason": "Block size mismatch|Invalid DSS|Disk full|..."
}
```

## 2. Read Operation (User → Disk)

### 2.1 read_block (User → Disk)

Sent by user to read a single block from a disk during the read operation.

**Request:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "read_block",
  "body": {
    "dss_name": "DemoDSS",
    "file_name": "tale-of-two-cities.txt",
    "stripe_number": 0
  }
}
```

**Fields:**
- `dss_name`: Name of the DSS
- `file_name`: Name of the file to read
- `stripe_number`: Stripe index (0-based)

**Response:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "read_block_response",
  "status_code": "SUCCESS",
  "reason": null,
  "body": {
    "block_type": "data|parity",
    "block_data_base64": "<base64-encoded-block>"
  }
}
```

**Failure Response:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "read_block_response",
  "status_code": "FAILURE",
  "reason": "File not found|Stripe not found|Block not found|..."
}
```

## 3. Disk Failure & Recovery

### 3.1 fail (User → Disk)

Sent by user to simulate a disk failure. Disk must delete all stored blocks.

**Request:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "fail",
  "body": {
    "dss_name": "DemoDSS"
  }
}
```

**Response:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "fail_response",
  "status_code": "SUCCESS",
  "reason": null,
  "body": {
    "blocks_deleted": 42
  }
}
```

### 3.2 read_block_for_recovery (User → Disk)

Sent by user to surviving disks to read blocks for reconstructing the failed disk. Identical to `read_block` but used in recovery context.

**Request:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "read_block_for_recovery",
  "body": {
    "dss_name": "DemoDSS",
    "file_name": "tale-of-two-cities.txt",
    "stripe_number": 0
  }
}
```

**Response:** Same as `read_block_response`

### 3.3 write_recovered_block (User → Disk)

Sent by user to the failed disk to write reconstructed blocks during recovery.

**Request:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "write_recovered_block",
  "body": {
    "dss_name": "DemoDSS",
    "file_name": "tale-of-two-cities.txt",
    "file_size": 760569,
    "owner": "Alice",
    "stripe_number": 0,
    "block_type": "data|parity",
    "block_data_base64": "<base64-encoded-block>"
  }
}
```

**Response:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "write_recovered_block_response",
  "status_code": "SUCCESS",
  "reason": null
}
```

### 3.4 delete_all (User → Disk)

Sent by user during decommission-dss to tell a disk to delete all blocks for a specific DSS.

**Request:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "delete_all",
  "body": {
    "dss_name": "DemoDSS"
  }
}
```

**Response:**
```json
{
  "version": 1,
  "message_id": "user-<pid>-<timestamp>-<counter>",
  "message_type": "delete_all_response",
  "status_code": "SUCCESS",
  "reason": null,
  "body": {
    "blocks_deleted": 42,
    "files_deleted": 3
  }
}
```

## 4. Implementation Notes

### 4.1 Threading

- Each disk runs 2 persistent threads:
  - Management port thread: handles manager messages (currently register/deregister)
  - Command port thread: handles P2P messages from users

- User spawns n threads per stripe:
  - For copy: each thread sends write_block to one disk
  - For read: each thread sends read_block to one disk
  - For recovery: threads read from n-1 disks and write to 1 disk

### 4.2 Block Data Encoding

- Block data is base64-encoded in JSON messages to handle binary data
- Actual block size = striping_unit bytes (before encoding)
- Padding: Last stripe may have null bytes (0x00) if file size doesn't align

### 4.3 XOR Parity Computation

For stripe with blocks B0, B1, ..., B(n-2) (data blocks) and Bp (parity):
```
Bp = B0 ⊕ B1 ⊕ ... ⊕ B(n-2)
```

To reconstruct missing block Bi:
```
Bi = B0 ⊕ B1 ⊕ ... ⊕ Bp (excluding Bi itself)
```

### 4.4 Parity Rotation

Parity block for stripe i goes to disk:
```
parity_disk = n - ((i mod n) + 1)
```

Examples (n=3):
- Stripe 0: parity on disk 2
- Stripe 1: parity on disk 1
- Stripe 2: parity on disk 0
- Stripe 3: parity on disk 2 (wraps around)

### 4.5 Error Injection (Read Operation)

For each block read:
1. Generate random integer k in [0, 100]
2. If k < p (error probability), flip one random bit in the block
3. Continue with parity check

If parity fails, retry reading the entire stripe.

### 4.6 Message Timeouts

- P2P operations should use the same timeout mechanism as manager operations
- Default: 3 seconds with 0 retries (can be configurable)
- On timeout, user should log error and potentially retry or fail the operation
