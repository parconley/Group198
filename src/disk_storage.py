"""
In-memory block storage for DSS disk process.

This module implements the block storage data structures as designed in
docs/disk-storage-design.md.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple


@dataclass(frozen=True)
class BlockRecord:
    """A single block (data or parity) stored on this disk."""

    dss_name: str
    file_name: str
    file_size: int
    owner: str
    stripe_number: int
    block_type: str  # "data" or "parity"
    block_data: bytes  # Raw block data (exactly striping_unit bytes)


@dataclass(frozen=True)
class FileMetadata:
    """Metadata about a file stored in this disk's blocks."""

    dss_name: str
    file_name: str
    file_size: int
    owner: str
    stripe_count: int  # Number of stripes for this file on this disk


class DiskStorage:
    """Thread-safe in-memory storage for disk blocks."""

    def __init__(self):
        # Main block storage: (dss_name, file_name, stripe_number) -> BlockRecord
        self.blocks: Dict[Tuple[str, str, int], BlockRecord] = {}

        # File metadata index: (dss_name, file_name) -> FileMetadata
        self.files: Dict[Tuple[str, str], FileMetadata] = {}

        # DSS membership: dss_name -> set of file_name
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

    def read_block(
        self, dss_name: str, file_name: str, stripe_number: int
    ) -> Optional[BlockRecord]:
        """Retrieve a block by key."""
        with self.lock:
            key = (dss_name, file_name, stripe_number)
            return self.blocks.get(key)

    def delete_dss(self, dss_name: str) -> Tuple[int, int]:
        """
        Delete all blocks for a DSS.

        Returns:
            (blocks_deleted, files_deleted)
        """
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
