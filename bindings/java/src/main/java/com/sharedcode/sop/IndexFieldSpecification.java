package com.sharedcode.sop;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IndexFieldSpecification {
    @JsonProperty("field_name")
    public String fieldName;

    @JsonProperty("ascending_sort_order")
    public boolean ascendingSortOrder = true;
}
