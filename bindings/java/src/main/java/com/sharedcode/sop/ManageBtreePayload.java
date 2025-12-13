package com.sharedcode.sop;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

class ManageBtreePayload<K, V> {
    @JsonProperty("items")
    public List<Item<K, V>> items;

    @JsonProperty("paging_info")
    public PagingInfo pagingInfo;
}
