
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

    c_id frozen <set<timeUUID>>,
    is_del boolean,
);

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
