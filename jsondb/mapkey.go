package jsondb

import (
	"context"
	"fmt"
	"sort"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/encoding"
)

// JsonDBMapKey wraps JsonDBAnyKey to support map[string]any keys with configurable index specifications.
type JsonDBMapKey struct {
	*JsonDBAnyKey[map[string]any, any]
	indexSpecification            *IndexSpecification
	defaultComparerSortedFields   []string
	defaultCoercedFieldsComparers []func(a, b any) int
}

// proxyComparer is used for delayed construction of the comparer until store metadata is available.
func (j *JsonDBMapKey) proxyComparer(mapX map[string]any, mapY map[string]any) int {
	if j.indexSpecification != nil {
		return j.indexSpecification.Comparer(mapX, mapY)
	}
	return j.defaultComparer(mapX, mapY)
}

// defaultComparer compares two flat maps by sorted field names using type-coerced field comparers.
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

// NewJsonBtreeMapKey creates a schema-less JSON B-Tree using map[string]any keys and optional index spec.
// This function is fully interoperable with other language bindings and offers high performance.
func NewJsonBtreeMapKey(ctx context.Context, config sop.DatabaseOptions, so sop.StoreOptions, t sop.Transaction, indexSpecification string) (*JsonDBMapKey, error) {
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
		so.CELexpression = indexSpecification
	}

	b3, err := NewJsonBtree[map[string]any, any](ctx, config, so, t, comparer)
	if err != nil {
		return nil, err
	}

	j.JsonDBAnyKey = b3
	return &j, nil
}

// OpenJsonBtreeMapKey opens an existing schema-less JSON B-Tree and reconstructs its index specification.
func OpenJsonBtreeMapKey(ctx context.Context, config sop.DatabaseOptions, name string, t sop.Transaction) (*JsonDBMapKey, error) {
	j := JsonDBMapKey{}

	b3, err := OpenJsonBtree[map[string]any, any](ctx, config, name, t, j.proxyComparer)
	if err != nil {
		return nil, err
	}

	// Resurrect the Key index specification originally provided when creating B-tree.
	iss := b3.GetStoreInfo().MapKeyIndexSpecification
	fmt.Printf("Resurrected IndexSpec: %s\n", iss)
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
