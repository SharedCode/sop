
create keyspace btree with replication = {'class':'SimpleStrategy', 'replication_factor':1};

use btree;

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
    des text,
    -- virtual id registry table name.
    reg_tbl text,
    -- blob table name.
    blob_tbl text,
    -- timestamp of the store.
    ts bigint,
    -- is value data in node segment.
    vdins boolean,
    -- is value data actively persisted.
    vdap boolean,
    -- is value data globally cached.
    vdgc boolean,
    -- leaf load balancing feature, defaults to false in Cassandra.
    llb boolean,
    -- is_del true specifies this store had been marked logically deleted.
    is_del boolean);

-- Node Blob demo table 1.
create table foo_b(
    id UUID PRIMARY KEY,
    node blob
);

-- Virtual Id Registry demo table 1.
create table foo_r(
    lid UUID PRIMARY KEY,
    is_idb boolean,
    p_ida UUID,
    p_idb UUID,
    ver int,
    wip_ts bigint,
    is_del boolean
);

-- Transaction Log table.
create table t_log(
    id UUID,
    c_f int,
    c_f_p blob,
    PRIMARY KEY(id, c_f)
);
