package jsondb

import (
	"context"
	"sort"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/encoding"
)

// JSON DB that can take in any JSON data marshalled as map[string]any on Key & Value pair.
type JsonDBMapKey struct {
	*JsonDBAnyKey[map[string]any, any]
	indexSpecification            *IndexSpecification
	defaultComparerSortedFields   []string
	defaultCoercedFieldsComparers []func(a, b any) int
}

func (j *JsonDBMapKey) proxyComparer(mapX map[string]any, mapY map[string]any) int {
	if j.indexSpecification != nil {
		return j.indexSpecification.Comparer(mapX, mapY)
	}
	return j.defaultComparer(mapX, mapY)
}

// Default Comparer of Items can compare two maps with no nested map.
func (j *JsonDBMapKey) defaultComparer(mapX map[string]any, mapY map[string]any) int {
	if j.defaultComparerSortedFields == nil {
		arr := make([]string, len(mapX))
		i := 0
		for k := range mapX {
			arr[i] = k
			i++
		}
		sort.Strings(arr)
		j.defaultComparerSortedFields = arr
		j.defaultCoercedFieldsComparers = make([]func(a any, b any) int, len(arr))
	}
	for i, k := range j.defaultComparerSortedFields {
		// Coerce the default Comparers needed by each field of the Key class (which is an entry in the MapKey).
		if j.defaultCoercedFieldsComparers[i] == nil {
			j.defaultCoercedFieldsComparers[i] = btree.CoerceComparer(mapX[k])
		}
		res := j.defaultCoercedFieldsComparers[i](mapX[k], mapY[k])
		if res != 0 {
			return res
		}
	}
	return 0
}

// Instantiates a Btree for schema-less usage. I.e. - JSONy type of data marshaled by Go as map[string]any
// data type for key & any value pairs.
func NewJsonBtreeMapKey(ctx context.Context, so sop.StoreOptions, t sop.Transaction, indexSpecification string) (*JsonDBMapKey, error) {
	var comparer btree.ComparerFunc[map[string]any]
	j := JsonDBMapKey{}
	if indexSpecification == "" {
		comparer = j.defaultComparer
	} else {
		// Create the comparer from the IndexSpecification JSON string that defines the fields list comprising the index (on key) & their sort order.
		var is IndexSpecification
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(indexSpecification), &is); err != nil {
			return nil, err
		}
		j.indexSpecification = &is
		comparer = is.Comparer
	}

	b3, err := NewJsonBtree[map[string]any, any](ctx, so, t, comparer)
	if err != nil {
		return nil, err
	}

	j.JsonDBAnyKey = b3
	return &j, nil
}

// Open an existing B-tree w/ option to using user provided CEL expression as comparer.
// If CEL expression is not provided, will use default comparer that compares each field of the key.
func OpenJsonBtreeMapKey(ctx context.Context, name string, t sop.Transaction) (*JsonDBMapKey, error) {
	j := JsonDBMapKey{}

	b3, err := OpenJsonBtree[map[string]any, any](ctx, name, t, j.proxyComparer)
	if err != nil {
		return nil, err
	}

	// Resurrect the CEL expression evaluator if CEL expression was originally provided when creating B-tree.
	iss := b3.GetStoreInfo().MapKeyIndexSpecification
	if iss != "" {
		// Create the comparer from the IndexSpecification JSON string that defines the fields list comprising the index (on key) & their sort order.
		var is IndexSpecification
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(iss), &is); err != nil {
			return nil, err
		}
		j.indexSpecification = &is
	}

	j.JsonDBAnyKey = b3
	return &j, nil
}
