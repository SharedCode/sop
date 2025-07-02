package jsondb

import (
	"github.com/SharedCode/sop/btree"
)

type IndexFieldSpecification struct {
	FieldName string `json:"field_name"`
	// Sort order can be ascending (true) or descending (false).
	AscendingSortOrder bool `json:"ascending_sort_order"`
}

// B-Tree Index specification.
type IndexSpecification struct {
	// Index Fields specification.
	IndexFields []IndexFieldSpecification `json:"index_fields"`
}

// Create a new IndexSpecification instance.
func NewIndexSpecification(indexFields []IndexFieldSpecification) *IndexSpecification {
	return &IndexSpecification{
		IndexFields: indexFields,
	}
}

// Comparer that consumes the IndexSpecification supplied by enduser.
func (idx *IndexSpecification) Comparer(x map[string]any, y map[string]any) int {
	for i := range idx.IndexFields {
		res := btree.Compare(x[idx.IndexFields[i].FieldName], y[idx.IndexFields[i].FieldName])
		if res != 0 {
			if !idx.IndexFields[i].AscendingSortOrder {
				// Reverse the result if Descending order.
				return res * -1
			}
			return res
		}
	}
	return 0
}
