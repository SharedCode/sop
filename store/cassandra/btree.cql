
create keyspace btree with replication = {'class':'SimpleStrategy', 'replication_factor':1};

-- Following are tables used as "index" of blob objects(e.g. b-tree node datablocks) in AWS S3.

create table store (
    -- name of the store.
    name text PRIMARY KEY,
    -- Logical Id of root node.
    root_lid UUID,
    -- slot count contains the number of items(key/value pairs) that can be stored in a node.
    slot_count int,
    -- total count of items that the store (or b-tree) contains.
    count bigint,
    -- unique specifies whether the store is unique(no item can have same key) or not.
    unique boolean,
    -- key info (optional).
    ki text,
    -- value info (optional).
    vi text,
    -- version number of the store.
    vrsn int,
    -- is_del true specifies this store had been marked logically deleted.
    is_del boolean);

-- Logical to Physical ID table will allow seamless transaction commit processing.
-- Only when the record here got updated that succeeding SOP Btree IO will reflect newly commited Node data changes.
create table virtual_id(
    lid UUID PRIMARY KEY,
    is_idb boolean,
    p_ida UUID,
    p_idb UUID,
    is_del boolean
);

-- deleted_items table contains items that are marked logically deleted.
-- These items get processed on schedule, like once a month or so, depends on your needs.
-- Delete SQL action are issued on these items so Cassandra can mark them with "tombstone"
-- and thus, gets deleted on next compaction schedule(e.g. - 10 days).
-- By doing this batch and delayed "deletion" of items, we are allowing a more optimal operation
-- for the Cassandra cluster.
create table deleted_items (
    -- t values can be: virtual_id, store
    t int,
    y int,
    m int,
    d int,
    h int,
    -- object id.
    oid UUID,
    Primary Key((t, y, m, d, h))
);
