using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Sop;

/// <summary>
/// Describes a foreign key relationship to another store.
/// </summary>
public class Relation
{
    /// <summary>
    /// The fields in the current store that act as the foreign key.
    /// </summary>
    [JsonPropertyName("source_fields")]
    public List<string> SourceFields { get; set; }

    /// <summary>
    /// The name of the target store usually "parent" table.
    /// </summary>
    [JsonPropertyName("target_store")]
    public string TargetStore { get; set; }

    /// <summary>
    /// The fields in the target store that match the source fields (usually the primary key).
    /// </summary>
    [JsonPropertyName("target_fields")]
    public List<string> TargetFields { get; set; }

    public Relation() { }

    public Relation(List<string> sourceFields, string targetStore, List<string> targetFields)
    {
        SourceFields = sourceFields;
        TargetStore = targetStore;
        TargetFields = targetFields;
    }
}
