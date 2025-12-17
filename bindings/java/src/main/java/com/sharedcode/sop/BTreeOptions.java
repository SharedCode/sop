package com.sharedcode.sop;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BTreeOptions {
    @JsonProperty("name")
    public String name = "";

    @JsonProperty("is_unique")
    public boolean isUnique = false;

    @JsonProperty("is_primitive_key")
    public boolean isPrimitiveKey = true;

    @JsonProperty("slot_length")
    public int slotLength = 500;

    @JsonProperty("description")
    public String description = "";

    @JsonProperty("index_specification")
    public String indexSpecification = "";

    @JsonIgnore
    public void setIndexSpecification(IndexSpecification spec) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            this.indexSpecification = mapper.writeValueAsString(spec);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize IndexSpecification", e);
        }
    }

    @JsonProperty("is_value_data_in_node_segment")
    public boolean isValueDataInNodeSegment = true;

    @JsonProperty("is_value_data_actively_persisted")
    public boolean isValueDataActivelyPersisted = false;

    @JsonProperty("is_value_data_globally_cached")
    public boolean isValueDataGloballyCached = false;

    @JsonProperty("leaf_load_balancing")
    public boolean leafLoadBalancing = false;

    @JsonProperty("transaction_id")
    public String transactionId;

    public BTreeOptions(String name) {
        this.name = name;
    }
}
