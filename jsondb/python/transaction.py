from datetime import timedelta
from enum import Enum


class TransactionMode(Enum):
    NoCheck = 0
    ForWriting = 1
    ForReading = 2


# 250, should generate 1MB file segment. Formula: 250 X 4096 = 1MB
# Given a 50 slot size per node, should be able to manage 825,000 B-Tree items (key/value pairs).
#
# Formula: 250 * 66 * 50 = 825,000
# Or if you use 100 slot size per node, 'will give you 1,650,000 items, or assuming you have about 65%
# b-tree utilization, 1,072,500 usable space.
MIN_HASH_MOD_VALUE = 250
# 750k, should generate 3GB file segment.  Formula: 750k X 4096 = 3GB
MAX_HASH_MOD_VALUE = 750000


class ErasureCodingConfig:
    def __init__(
        self,
        data_shards_count: int,
        parity_shards_count: int,
        base_folder_paths_across_drives: list[str],
        repair_corrupted_shards: bool,
    ):
        self.data_shards_count = data_shards_count
        self.parity_shards_count = parity_shards_count
        self.base_folder_paths_across_drives = base_folder_paths_across_drives
        self.repair_corrupted_shards = repair_corrupted_shards


class TransationOptions:
    glolbal_erasure_config: ErasureCodingConfig

    def __init__(
        self,
        stores_folders: str,
        mode: TransactionMode,
        max_time: timedelta,
        registry_hash_mod: int,
        erasure_config: dict[str, ErasureCodingConfig],
    ):
        if erasure_config == None:
            erasure_config = TransationOptions.glolbal_erasure_config
        if len(stores_folders) != 2:
            raise "'stores_folders' need to be array of two strings(drive/folder paths)"

        if registry_hash_mod < MIN_HASH_MOD_VALUE:
            registry_hash_mod = MIN_HASH_MOD_VALUE
        if registry_hash_mod > MAX_HASH_MOD_VALUE:
            registry_hash_mod = MAX_HASH_MOD_VALUE

        # Default to 15 minute commit time.
        if max_time < 0:
            max_time = 15 * timedelta.minutes

        # Base folder where the Stores (registry, blob & store repository) subdirectories & files
        # will be created in. This is expected to be two element array, the 2nd element specifies
        # a 2nd folder for use in SOP replication.
        self.store_folders = stores_folders
        # Transaction Mode can be Read-only or Read-Write.
        self.mode = mode
        # Transaction maximum "commit" time. If commits takes longer than this then transaction will roll back.
        self.max_time = max_time
        # Registry hash modulo value used for hashing.
        self.registry_hash_mod = registry_hash_mod
        # Erasure Config contains config data useful for Erasure Coding based file IO (& replication).
        self.erasure_config = erasure_config


class Transaction:
    def __init__(self, options: TransationOptions):
        self.options = options

    @classmethod
    def begin():
        return

    @classmethod
    def commit():
        return

    @classmethod
    def rollback():
        return
