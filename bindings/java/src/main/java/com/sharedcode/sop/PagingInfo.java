package com.sharedcode.sop;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PagingInfo {
    @JsonProperty("page_offset")
    public int pageOffset;

    @JsonProperty("page_size")
    public int pageSize = 20;

    @JsonProperty("fetch_count")
    public int fetchCount;

    @JsonProperty("direction")
    public int direction;
}
