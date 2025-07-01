package jsondb

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/cel"
)

// JSON DB that can take in any JSON data marshalled as map[string]any on Key & Value pair.
type JsonDBMapKey struct {
	*JsonDBAnyKey[map[string]any, any]
	evaluator *cel.Evaluator
}

func (j *JsonDBMapKey) proxyComparer(mapX map[string]any, mapY map[string]any) int {
	if j.evaluator != nil {
		return j.celComparer(mapX, mapY)
	}
	return defaultComparer(mapX, mapY)
}

// Comparer for map[string]any key type using CEL expression.
func (j *JsonDBMapKey) celComparer(mapX map[string]any, mapY map[string]any) int {
	r, err := j.evaluator.Evaluate(mapX, mapY)
	if err != nil {
		j.compareError = err
	}
	return r
}

// Default Comparer of Items can compare two maps with no nested map.
func defaultComparer(mapX map[string]any, mapY map[string]any) int {
	for k, v := range mapX {
		i := btree.Compare(v, mapY[k])
		if i != 0 {
			return i
		}
	}
	return 0
}

// Instantiates a Btree for schema-less usage. I.e. - JSONy type of data marshaled by Go as map[string]any
// data type for key & value pairs.
// And using user provided CEL expression as comparer. If not provided, will use default comparer that compares each field of the key.
func NewJsonBtreeMapKey(ctx context.Context, so sop.StoreOptions, t sop.Transaction, celExpressionComparer string) (*JsonDBMapKey, error) {
	var comparer btree.ComparerFunc[map[string]any]
	j := JsonDBMapKey{}
	if celExpressionComparer == "" {
		comparer = defaultComparer
	} else {
		e, err := cel.NewEvaluator("comparer", celExpressionComparer)
		if err != nil {
			return nil, err
		}
		j.evaluator = e
		comparer = j.celComparer
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
	ce := b3.GetStoreInfo().CELexpression
	if ce != "" {
		e, err := cel.NewEvaluator("comparer", ce)
		if err != nil {
			return nil, err
		}
		j.evaluator = e
	}

	j.JsonDBAnyKey = b3
	return &j, nil
}
