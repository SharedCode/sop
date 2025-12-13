package com.sharedcode.sop;

import com.fasterxml.jackson.annotation.JsonProperty;

class ManageBtreeMetaData {
    @JsonProperty("is_primitive_key")
    public boolean isPrimitiveKey;

    @JsonProperty("transaction_id")
    public String transactionId;

    @JsonProperty("btree_id")
    public String btreeId;
}
