using System;
using System.Text.Json.Serialization;

namespace Sop;

public class Item<TK, TV>
{
    [JsonPropertyName("key")]
    public TK Key { get; set; }

    [JsonPropertyName("value")]
    public TV Value { get; set; }

    [JsonPropertyName("id")]
    public string Id { get; set; }

    public Item() { }

    public Item(TK key, TV value)
    {
        Key = key;
        Value = value;
    }
}
