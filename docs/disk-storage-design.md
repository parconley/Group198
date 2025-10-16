# Disk Block Storage Design

This document defines the in-memory data structures used by disk processes to store blocks.

## Overview

Each disk maintains in-memory storage for blocks from potentially multiple files across different DSSs. The storage must support:
- Storing data and parity blocks with metadata
- Fast lookup by (dss_name, file_name, stripe_number)
- Tracking file metadata (size, owner)
- Deleting blocks for specific DSS or failed disk scenarios

## Data Structures

### BlockRecord

Represents a single block (data or parity) stored on this disk.

```python
@dataclass(frozen=True)
class BlockRecord:
    dss_name: str
    file_name: str
    file_size: int
    owner: str
    stripe_number: int
    block_type: str  # "data" or "parity"
    block_data: bytes  # Raw block data (exactly striping_unit bytes)
```

**Key:** `(dss_name, file_name, stripe_number)`

**Design Rationale:**
- `frozen=True` makes records immutable and hashable
- `block_data` stored as raw bytes (not base64) for efficient XOR operations
- Each block stores full file metadata for reconstruction scenarios

### FileMetadata

Tracks metadata about files stored in the disk's blocks (for quick queries).

```python
@dataclass(frozen=True)
class FileMetadata:
    dss_name: str
    file_name: str
    file_size: int
    owner: str
    stripe_count: int  # Number of stripes for this file on this disk
```

**Key:** `(dss_name, file_name)`

### DiskStorage

Main storage class for the disk process.

```python
class DiskStorage:
    def __init__(self):
        # Main block storage: (dss_name, file_name, stripe_number) -> BlockRecord
        self.blocks: Dict[Tuple[str, str, int], BlockRecord] = {}

        # File metadata index: (dss_name, file_name) -> FileMetadata
        self.files: Dict[Tuple[str, str], FileMetadata] = {}

        # DSS membership: dss_name -> set of (file_name)
        self.dss_files: Dict[str, Set[str]] = {}

        # Thread-safe lock for concurrent access
        self.lock: threading.Lock = threading.Lock()

    def write_block(self, record: BlockRecord) -> None:
        """Store a block and update metadata indices."""
        with self.lock:
            key = (record.dss_name, record.file_name, record.stripe_number)
            self.blocks[key] = record

            # Update file metadata
            file_key = (record.dss_name, record.file_name)
            if file_key not in self.files:
                self.files[file_key] = FileMetadata(
                    dss_name=record.dss_name,
                    file_name=record.file_name,
                    file_size=record.file_size,
                    owner=record.owner,
                    stripe_count=1
                )
                # Add to DSS index
                if record.dss_name not in self.dss_files:
                    self.dss_files[record.dss_name] = set()
                self.dss_files[record.dss_name].add(record.file_name)
            else:
                # Update stripe count
                existing = self.files[file_key]
                self.files[file_key] = FileMetadata(
                    dss_name=existing.dss_name,
                    file_name=existing.file_name,
                    file_size=existing.file_size,
                    owner=existing.owner,
                    stripe_count=existing.stripe_count + 1
                )

    def read_block(self, dss_name: str, file_name: str, stripe_number: int) -> Optional[BlockRecord]:
        """Retrieve a block by key."""
        with self.lock:
            key = (dss_name, file_name, stripe_number)
            return self.blocks.get(key)

    def delete_dss(self, dss_name: str) -> Tuple[int, int]:
        """Delete all blocks for a DSS. Returns (blocks_deleted, files_deleted)."""
        with self.lock:
            files_in_dss = self.dss_files.get(dss_name, set()).copy()
            blocks_deleted = 0

            for file_name in files_in_dss:
                # Delete all blocks for this file
                keys_to_delete = [
                    key for key in self.blocks.keys()
                    if key[0] == dss_name and key[1] == file_name
                ]
                for key in keys_to_delete:
                    del self.blocks[key]
                    blocks_deleted += 1

                # Remove file metadata
                file_key = (dss_name, file_name)
                self.files.pop(file_key, None)

            # Remove DSS index
            self.dss_files.pop(dss_name, None)

            return (blocks_deleted, len(files_in_dss))

    def get_all_files(self) -> List[FileMetadata]:
        """Get metadata for all files stored on this disk."""
        with self.lock:
            return list(self.files.values())

    def get_storage_stats(self) -> Dict[str, int]:
        """Get storage statistics."""
        with self.lock:
            return {
                "total_blocks": len(self.blocks),
                "total_files": len(self.files),
                "total_dss": len(self.dss_files)
            }
```

## Implementation Notes

### Thread Safety

- All public methods use `self.lock` to ensure thread-safe access
- Command port thread and management port thread may access storage concurrently
- Lock is acquired once per operation to minimize contention

### Memory Efficiency

For a DSS with:
- 3 files (avg 500 KB each)
- n = 3 disks
- striping_unit = 256 bytes

Each disk stores approximately:
- Blocks per file: ceil(500000 / (2 * 256)) = 977 stripes
- Total blocks: 3 files × 977 stripes = 2,931 blocks
- Memory per block: ~256 bytes data + ~100 bytes metadata = ~356 bytes
- Total memory: ~1 MB

This scales linearly with file count and size, which is acceptable for in-memory storage.

### Lookup Performance

- Block lookup: O(1) via dict with composite key
- File metadata lookup: O(1) via dict
- DSS deletion: O(b) where b = blocks in DSS
- List all files: O(f) where f = file count

### Alternative Designs Considered

**Option A: Nested Dicts**
```python
self.storage: Dict[str, Dict[str, Dict[int, BlockRecord]]]
# storage[dss_name][file_name][stripe_number] = block
```
- Pro: Intuitive hierarchical structure
- Con: More complex deletion logic, null checks at each level

**Option B: SQLite Database**
```sql
CREATE TABLE blocks (
    dss_name TEXT,
    file_name TEXT,
    stripe_number INTEGER,
    block_type TEXT,
    block_data BLOB,
    PRIMARY KEY (dss_name, file_name, stripe_number)
);
```
- Pro: Persistence, complex queries
- Con: Project specifies in-memory storage, added complexity

**Selected: Flat Dict with Composite Keys**
- Simple and fast
- Easy thread-safety with single lock
- Minimal code complexity
- Meets project requirements

## Integration with disk.py

The `DiskStorage` class will be instantiated in `disk.py` and accessed by the command port handler thread. Example:

```python
# In disk.py
class DiskServer:
    def __init__(self, ...):
        self.storage = DiskStorage()
        self.command_thread = threading.Thread(target=self._command_port_loop)
        self.management_thread = threading.Thread(target=self._management_port_loop)

    def _command_port_loop(self):
        # Handle write_block, read_block, fail, etc.
        # Access self.storage
        pass
```
