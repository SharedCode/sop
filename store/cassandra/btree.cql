
create keyspace btree with replication = {'class':'SimpleStrategy', 'replication_factor':1};

create table store (
    name text PRIMARY KEY, 
    root_id timeUUID,
    slot_count int,
    unique boolean,
    is_del boolean);

create table node (
    id timeUUID PRIMARY KEY,

    s frozen <set<tuple<text, int, blob>>>,
    s_tt frozen <set<tuple<text, int, text>>>,
    s_bb frozen <set<tuple<blob, int, blob>>>,
    s_tid frozen <set<tuple<text, int, timeUUID>>>,
    s_bid frozen <set<tuple<blob, int, timeUUID>>>,

    // children logical IDs
    c_id frozen <set<timeUUID>>,
    is_del boolean,
);

// node datablocks can be used for storing very large BLOBs (vlblobs).
// SOP's datablock technology solves managing vlblobs without "streaming" req't.
create table node_datablocks (
    id timeUUID PRIMARY KEY,
    // if set, contains all other "block physical IDs" (timeUUID) (a.k.a. map) storing the vlblob data.
    // otherwise, data fit into a single blob (s_block).
    s_b_map frozen <set<timeUUID>>,
    s_block blob,
    // children logical IDs
    c_id frozen <set<timeUUID>>,
    is_del boolean,
);

// Logical to Physical ID table will allow seamless transaction commit processing.
// Only when the record here got updated that succeeding SOP Btree IO will reflect newly commited Node data changes.
create table virtual_id(
    l_id timeUUID PRIMARY KEY,
    is_idb boolean,
    p_ida timeUUID,
    p_idb timeUUID,
    is_del boolean
);

create table slot_value(
    id timeUUID Primary Key,
    v blob,
    is_del boolean,
);

create table recycle_bin (
    y int,
    m int,
    d int,
    h int,
    t int,          //(option) default to node type
    id timeUUID,
    lock_dt date,
    is_del boolean,
    Primary Key((t, is_del, y, m, d, h))
);

create table transaction_sandbox(
    id timeUUID,
    store text,
    seq timeUUID,
    // read lock on kvp, add, update, delete
    act int,
    vrsn int,      // current value version, should be same, if act=update,delete
    c_kv blob,     // current kvp
    n_kv blob,     // new kvp
    is_del boolean,
    Primary Key((id, store, seq))
);
