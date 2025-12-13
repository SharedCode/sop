package com.sharedcode.sop;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Item<K, V> {
    @JsonProperty("key")
    public K key;

    @JsonProperty("value")
    public V value;

    @JsonProperty("id")
    public String id;

    public Item() {}

    public Item(K key, V value) {
        this.key = key;
        this.value = value;
    }
}
