package jsondb

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/cel"
	"github.com/SharedCode/sop/in_red_fs"
)

// JSON DB that can take in any JSON data marshalled as map[string]any on Key & Value pair.
type JsonDB struct {
	btree.BtreeInterface[map[string]any, map[string]any]
	evaluator    *cel.Evaluator
	compareError error
}

// Comparer for map[string]any key type using CEL expression.
func (j *JsonDB) CELComparer(mapX map[string]any, mapY map[string]any) int {
	r, err := j.evaluator.Evaluate(mapX, mapY)
	if err != nil {
		j.compareError = err
	}
	return r
}

// Default Comparer of Items can compare two maps with no nested map.
func DefaultComparer(a map[string]any, b map[string]any) int {
	for k, v := range a {
		i := btree.Compare(v, b[k])
		if i != 0 {
			return i
		}
	}
	return 0
}

// Instantiates a Btree for schema-less usage. I.e. - JSONy type of data marshaled by Go as map[string]any
// data type for key & value pairs.
// And using user provided CEL expression as comparer.
func NewBtree(ctx context.Context, so sop.StoreOptions, t sop.Transaction, celExpressionComparer string) (*JsonDB, error) {
	var comparer btree.ComparerFunc[map[string]any]
	j := JsonDB{}
	if celExpressionComparer == "" {
		comparer = DefaultComparer
	} else {
		e, err := cel.NewEvaluator("comparer", celExpressionComparer)
		if err != nil {
			return nil, err
		}
		j.evaluator = e
		comparer = j.CELComparer
	}

	b3, err := in_red_fs.NewBtreeWithReplication[map[string]any, map[string]any](ctx, so, t, comparer)
	if err != nil {
		return nil, err
	}

	j.BtreeInterface = b3
	return &j, nil
}

// Open an existing B-tree & use its StoreInfo.Description as the comparer's CEL expression.
func OpenBtree(ctx context.Context, name string, t sop.Transaction, celExpressionComparer string) (*JsonDB, error) {
	var comparer btree.ComparerFunc[map[string]any]
	j := JsonDB{}
	if celExpressionComparer == "" {
		comparer = DefaultComparer
	} else {
		e, err := cel.NewEvaluator("comparer", celExpressionComparer)
		if err != nil {
			return nil, err
		}
		j.evaluator = e
		comparer = j.CELComparer
	}

	b3, err := in_red_fs.OpenBtree[map[string]any, map[string]any](ctx, name, t, comparer)
	if err != nil {
		return nil, err
	}
	j.BtreeInterface = b3
	return &j, nil
}
