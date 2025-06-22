package jsondb

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/cel"
	"github.com/SharedCode/sop/in_red_fs"
)

// JSON DB that can take in any JSON data marshalled as map[string]any on Key & Value pair.
type JsonDB struct {
	btree.BtreeInterface[map[string]any, map[string]any]
	evaluator *cel.Evaluator
}

// Comparer for map[string]any key type.
func (j *JsonDB) Comparer(mapX map[string]any, mapY map[string]any) int {
	r, _ := j.evaluator.Evaluate(mapX, mapY)
	return r
}

// Instantiates a Btree for schema-less usage. I.e. - JSONy type of data marshaled by Go as map[string]any
// data type for key & value pairs.
// And using user provided CEL expression as comparer.
func NewBtree(ctx context.Context, so sop.StoreOptions, t sop.Transaction, comparerCELexpression string) (*JsonDB, error) {
	if comparerCELexpression == "" {
		return nil, fmt.Errorf("comparerCELexpression needs to be valid CEL expression that compares map[string]any x & y")
	}
	// Store Comparer CEL expression on B-tree/table's Description field.
	so.Description = comparerCELexpression
	e, err := cel.NewEvaluator("comparer", comparerCELexpression)
	if err != nil {
		return nil, err
	}

	j := JsonDB{
		evaluator: e,
	}

	b3, err := in_red_fs.NewBtreeWithReplication[map[string]any, map[string]any](ctx, so, t, j.Comparer)
	if err != nil {
		return nil, err
	}

	j.BtreeInterface = b3
	return &j, nil
}

// Open an existing B-tree & use its StoreInfo.Description as the comparer's CEL expression.
func OpenBtree(ctx context.Context, name string, t sop.Transaction) (*JsonDB, error) {
	j := JsonDB{}
	b3, err := in_red_fs.OpenBtree[map[string]any, map[string]any](ctx, name, t, j.Comparer)
	if err != nil {
		return nil, err
	}
	ce := b3.GetStoreInfo().Description
	j.evaluator, err = cel.NewEvaluator("comparer", ce)
	if err != nil {
		return nil, err
	}
	j.BtreeInterface = b3
	return &j, nil
}
