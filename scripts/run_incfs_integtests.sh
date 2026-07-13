#!/bin/bash
SOP_RUN_INCFS_IT=1 go test -v -tags=integration -count=1 ./incfs/integrationtests/...

#dropping all Cassandra user keyspaces:
#cqlsh -e "SELECT keyspace_name FROM system_schema.keyspaces" | grep -vE "keyspace_name|system|system_auth|system_distributed|system_schema|system_traces|system_views|system_virtual_schema|^-" | tr -d ' ' | xargs -I {} cqlsh -e "DROP KEYSPACE {};"
