
create keyspace btree with replication = {'class':'SimpleStrategy', 'replication_factor':1};

create table store (
    -- name of the store.
    name text PRIMARY KEY,
    -- Logical Id of root node.
    root_id UUID,
    -- slot count contains the number of items(key/value pairs) that can be stored in a node.
    slot_count int,
    -- total count of items that the store (or b-tree) contains.
    count bigint,
    -- unique specifies whether the store is unique(on key) or not.
    unique boolean,
    -- description (optional).
    desc text,
    -- virtual id table name.
    vid_tbl text,
    -- store path, e.g. bucket path/name, folder path.
    path
    -- version number of the store.
    ts bigint,
    -- is value data in node segment.
    vdins boolean,
    -- is_del true specifies this store had been marked logically deleted.
    is_del boolean);

-- Virtual Id Registry demo table 1.
create table vid_1(
    lid UUID PRIMARY KEY,
    is_idb boolean,
    p_ida UUID,
    p_idb UUID,
    ts bigint,
    wip_ts bigint,
    is_del boolean
);

-- Virtual Id Registry demo table 2.
create table vid_2(
    lid UUID PRIMARY KEY,
    is_idb boolean,
    p_ida UUID,
    p_idb UUID,
    ts bigint,
    wip_ts bigint,
    is_del boolean
);
