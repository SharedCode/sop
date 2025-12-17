package com.sharedcode.sop;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.ArrayList;

public class IndexSpecification {
    @JsonProperty("index_fields")
    public List<IndexFieldSpecification> indexFields = new ArrayList<>();
}
