import sop
from sop.database import Database, DatabaseOptions, DatabaseType, RedisCacheConfig

def main():
    ctx = sop.Context()

    print("--- Multi-Tenancy with Redis URLs Demo ---")

    # 1. Configure Tenant A (Redis DB 0)
    # Using the new URL configuration style.
    print("Configuring Tenant A (Redis DB 0)...")
    opts_a = DatabaseOptions(
        type=DatabaseType.Clustered,
        redis_config=RedisCacheConfig(url="redis://localhost:6379/0"),
        keyspace="tenant_a" # Optional: Separate Cassandra keyspace if needed
    )
    db_a = Database(opts_a)

    # 2. Configure Tenant B (Redis DB 1)
    # Using the new URL configuration style.
    print("Configuring Tenant B (Redis DB 1)...")
    opts_b = DatabaseOptions(
        type=DatabaseType.Clustered,
        redis_config=RedisCacheConfig(url="redis://localhost:6379/1"),
        keyspace="tenant_b" # Optional: Separate Cassandra keyspace if needed
    )
    db_b = Database(opts_b)

    # 3. Transaction on Tenant A
    print("Writing to Tenant A...")
    with db_a.begin_transaction(ctx) as tx_a:
        store_a = db_a.new_btree(ctx, "config", tx_a)
        store_a.add(ctx, sop.btree.Item(key="app_name", value="Tenant A App"))
        # Commit happens automatically on exit of 'with' block

    # 4. Transaction on Tenant B
    print("Writing to Tenant B...")
    with db_b.begin_transaction(ctx) as tx_b:
        store_b = db_b.new_btree(ctx, "config", tx_b)
        store_b.add(ctx, sop.btree.Item(key="app_name", value="Tenant B App"))
    
    # 5. Verify Data Isolation
    print("Verifying Isolation...")
    
    # Check Tenant A
    with db_a.begin_transaction(ctx) as tx_a:
        store_a = db_a.open_btree(ctx, "config", tx_a)
        if store_a.find(ctx, "app_name"):
            item = store_a.get_current_item(ctx)
            print(f"Tenant A Config: {item.key} -> {item.value}")
            assert item.value == "Tenant A App"

    # Check Tenant B
    with db_b.begin_transaction(ctx) as tx_b:
        store_b = db_b.open_btree(ctx, "config", tx_b)
        if store_b.find(ctx, "app_name"):
            item = store_b.get_current_item(ctx)
            print(f"Tenant B Config: {item.key} -> {item.value}")
            assert item.value == "Tenant B App"

    print("Successfully demonstrated multi-tenancy with Redis URLs!")

if __name__ == "__main__":
    main()
