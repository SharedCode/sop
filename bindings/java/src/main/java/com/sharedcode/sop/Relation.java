package com.sharedcode.sop;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Relation {
    @JsonProperty("source_fields")
    public List<String> sourceFields;

    @JsonProperty("target_store")
    public String targetStore;

    @JsonProperty("target_fields")
    public List<String> targetFields;

    public Relation() {
    }

    public Relation(List<String> sourceFields, String targetStore, List<String> targetFields) {
        this.sourceFields = sourceFields;
        this.targetStore = targetStore;
        this.targetFields = targetFields;
    }
}
