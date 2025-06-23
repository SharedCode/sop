import datetime

from typing import Dict
from enum import Enum
class TransactionMode(Enum):
    NoCheck = 0
    ForWriting = 1
    ForReading = 2

class ErasureCodingConfig:
    def __init__(self, data_shards_count: int, parity_shards_count: int, base_folder_paths_across_drives: list, repair_corrupted_shards: bool):
        self.data_shards_count = data_shards_count
        self.parity_shards_count = parity_shards_count
        self.base_folder_paths_across_drives = base_folder_paths_across_drives
        self.repair_corrupted_shards = repair_corrupted_shards

class TransationOptions:
    def __init__(self, store_folder: str, mode: TransactionMode, max_time: datetime.datetime.minute, registry_hash_mod: int, erasure_config: Dict[str, ErasureCodingConfig]):
        # Base folder where the Stores (registry, blob & store repository) subdirectories & files
        # will be created in. This is expected to be two element array, the 2nd element specifies
        # a 2nd folder for use in SOP replication.
        self.store_folder = store_folder
        # Transaction Mode can be Read-only or Read-Write.
        self.mode = mode
        # Transaction maximum "commit" time. If commits takes longer than this then transaction will roll back.
        self.max_time = max_time
        # Registry hash modulo value used for hashing.
        self.registry_hash_mod = registry_hash_mod
        # Erasure Config contains config data useful for Erasure Coding based file IO (& replication).
        self.erasure_config = erasure_config
