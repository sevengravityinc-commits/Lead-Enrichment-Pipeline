"""
Checkpoint Manager

Provides save/resume functionality for long-running batch jobs.
Saves progress every N rows to allow resumption after failures.

Usage:
    from checkpoint_manager import CheckpointManager

    # Create checkpoint for a job
    cp = CheckpointManager("enrich_leads_job_123")

    # Check if resuming
    if cp.has_checkpoint():
        last_row, results = cp.load()
        print(f"Resuming from row {last_row}")
    else:
        last_row = 0
        results = []

    # Process rows
    for i, row in enumerate(rows[last_row:], start=last_row):
        result = process_row(row)
        results.append(result)

        # Save checkpoint every 50 rows
        if (i + 1) % 50 == 0:
            cp.save(i + 1, results)

    # Clear checkpoint on success
    cp.clear()
"""

import os
import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional
from pathlib import Path


class CheckpointManager:
    """
    Manages checkpoints for long-running batch jobs.

    Features:
    - Save progress at regular intervals
    - Resume from last checkpoint after failure
    - Track job metadata (start time, input file, etc.)
    - Automatic checkpoint file naming
    """

    def __init__(
        self,
        job_id: str,
        checkpoint_dir: str = ".tmp/checkpoints",
        save_interval: int = 50
    ):
        """
        Initialize checkpoint manager.

        Args:
            job_id: Unique identifier for this job
            checkpoint_dir: Directory to store checkpoint files
            save_interval: How often to auto-save (every N rows)
        """
        self.job_id = job_id
        self.checkpoint_dir = checkpoint_dir
        self.save_interval = save_interval
        self.checkpoint_path = os.path.join(checkpoint_dir, f"{job_id}.checkpoint.json")

        # Ensure directory exists
        os.makedirs(checkpoint_dir, exist_ok=True)

        # Job metadata
        self.metadata: Dict[str, Any] = {}
        self.start_time = datetime.now(timezone.utc)

    @classmethod
    def for_file(cls, input_file: str, job_type: str = "process") -> "CheckpointManager":
        """
        Create checkpoint manager for a specific input file.
        Job ID is derived from file path and modification time.
        """
        # Create deterministic job ID from file
        stat = os.stat(input_file)
        file_info = f"{input_file}_{stat.st_mtime}_{stat.st_size}"
        file_hash = hashlib.md5(file_info.encode()).hexdigest()[:8]
        job_id = f"{job_type}_{os.path.basename(input_file)}_{file_hash}"

        cp = cls(job_id)
        cp.metadata["input_file"] = input_file
        cp.metadata["job_type"] = job_type

        return cp

    def has_checkpoint(self) -> bool:
        """Check if a checkpoint exists for this job"""
        return os.path.exists(self.checkpoint_path)

    def save(
        self,
        last_row: int,
        results: List[Any],
        extra_data: Dict[str, Any] = None
    ) -> str:
        """
        Save checkpoint.

        Args:
            last_row: Last successfully processed row index
            results: Accumulated results so far
            extra_data: Additional data to save

        Returns:
            Path to checkpoint file
        """
        checkpoint = {
            "job_id": self.job_id,
            "last_row": last_row,
            "results_count": len(results),
            "results": results,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "started_at": self.start_time.isoformat(),
            "metadata": self.metadata
        }

        if extra_data:
            checkpoint["extra_data"] = extra_data

        # Write atomically (write to temp, then rename)
        temp_path = self.checkpoint_path + ".tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=2, default=str)

        os.replace(temp_path, self.checkpoint_path)

        return self.checkpoint_path

    def load(self) -> Tuple[int, List[Any], Dict[str, Any]]:
        """
        Load checkpoint.

        Returns:
            Tuple of (last_row, results, extra_data)
        """
        if not self.has_checkpoint():
            return 0, [], {}

        with open(self.checkpoint_path, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)

        last_row = checkpoint.get("last_row", 0)
        results = checkpoint.get("results", [])
        extra_data = checkpoint.get("extra_data", {})

        # Restore metadata
        self.metadata = checkpoint.get("metadata", {})

        print(f"Loaded checkpoint: job={self.job_id}, last_row={last_row}, results={len(results)}")

        return last_row, results, extra_data

    def clear(self) -> None:
        """Clear checkpoint (call on successful completion)"""
        if os.path.exists(self.checkpoint_path):
            os.remove(self.checkpoint_path)
            print(f"Cleared checkpoint: {self.checkpoint_path}")

    def should_save(self, current_row: int) -> bool:
        """Check if checkpoint should be saved at current row"""
        return (current_row + 1) % self.save_interval == 0

    def get_progress(self) -> Optional[Dict]:
        """Get progress info from checkpoint if exists"""
        if not self.has_checkpoint():
            return None

        with open(self.checkpoint_path, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)

        return {
            "job_id": checkpoint.get("job_id"),
            "last_row": checkpoint.get("last_row"),
            "results_count": checkpoint.get("results_count"),
            "saved_at": checkpoint.get("saved_at"),
            "started_at": checkpoint.get("started_at")
        }


class BatchProcessor:
    """
    Helper class for processing batches with automatic checkpointing.

    Usage:
        def process_fn(row):
            # Your processing logic
            return enriched_row

        processor = BatchProcessor("enrich_job", rows, process_fn)
        results = processor.run()
    """

    def __init__(
        self,
        job_id: str,
        items: List[Any],
        process_fn: callable,
        checkpoint_interval: int = 50,
        on_progress: callable = None
    ):
        """
        Initialize batch processor.

        Args:
            job_id: Unique job identifier
            items: List of items to process
            process_fn: Function to call for each item
            checkpoint_interval: Save checkpoint every N items
            on_progress: Optional callback(current, total, result)
        """
        self.checkpoint = CheckpointManager(job_id, save_interval=checkpoint_interval)
        self.items = items
        self.process_fn = process_fn
        self.on_progress = on_progress

    def run(self) -> List[Any]:
        """
        Run batch processing with automatic checkpointing.

        Returns:
            List of processed results
        """
        total = len(self.items)

        # Check for existing checkpoint
        if self.checkpoint.has_checkpoint():
            start_row, results, _ = self.checkpoint.load()
            print(f"Resuming from row {start_row} (already processed {len(results)})")
        else:
            start_row = 0
            results = []

        # Process remaining items
        for i in range(start_row, total):
            item = self.items[i]

            try:
                result = self.process_fn(item)
                results.append(result)

                # Progress callback
                if self.on_progress:
                    self.on_progress(i + 1, total, result)

                # Checkpoint
                if self.checkpoint.should_save(i):
                    self.checkpoint.save(i + 1, results)
                    print(f"Checkpoint saved at row {i + 1}/{total}")

            except Exception as e:
                # Save checkpoint before re-raising
                self.checkpoint.save(i, results, {"error": str(e), "failed_row": i})
                print(f"Error at row {i}: {e}")
                print(f"Checkpoint saved. Resume with same job_id to continue.")
                raise

        # Clear checkpoint on success
        self.checkpoint.clear()
        print(f"Completed processing {total} items")

        return results


def list_checkpoints(checkpoint_dir: str = ".tmp/checkpoints") -> List[Dict]:
    """List all existing checkpoints"""
    if not os.path.exists(checkpoint_dir):
        return []

    checkpoints = []
    for f in os.listdir(checkpoint_dir):
        if f.endswith(".checkpoint.json"):
            path = os.path.join(checkpoint_dir, f)
            with open(path, "r", encoding="utf-8") as file:
                data = json.load(file)
                checkpoints.append({
                    "job_id": data.get("job_id"),
                    "last_row": data.get("last_row"),
                    "results_count": data.get("results_count"),
                    "saved_at": data.get("saved_at"),
                    "file": path
                })

    return checkpoints


def main():
    """List existing checkpoints"""
    checkpoints = list_checkpoints()

    if not checkpoints:
        print("No checkpoints found")
        return

    print(f"Found {len(checkpoints)} checkpoint(s):\n")
    for cp in checkpoints:
        print(f"  Job: {cp['job_id']}")
        print(f"    Last row: {cp['last_row']}")
        print(f"    Results: {cp['results_count']}")
        print(f"    Saved: {cp['saved_at']}")
        print(f"    File: {cp['file']}")
        print()


if __name__ == "__main__":
    main()
