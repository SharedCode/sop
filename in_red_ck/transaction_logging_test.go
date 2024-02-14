package in_red_ck

import (
	"fmt"
	"strings"
	"testing"
)

func Test_TLog_Rollback(t *testing.T) {
	trans, _ := NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()

	b3, _ := NewBtree[PersonKey, Person](ctx, "tlogtable", nodeSlotLength, false, false, false, "", trans)

	pk, p := newPerson("joe", "shroeger", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.Commit(ctx)

	trans, _ = NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()

	pk, p = newPerson("joe", "shroeger", "male", "email2", "phone2")
	b3.Update(ctx, pk, p)

	trans.Rollback(ctx)

	trans, _ = NewMockTransactionWithLogging(t, false, -1)
	trans.Begin()
	b3, _ = OpenBtree[PersonKey, Person](ctx, "tlogtable", trans)
	pk, p = newPerson("joe", "shroeger", "male", "email", "phone")

	b3.FindOne(ctx, pk, false)
	v, _ := b3.GetCurrentValue(ctx)

	if v.Email != "email" {
		t.Errorf("Rollback did not restore person record, email got = %s, want = 'email'.", v.Email)
	}
	trans.Commit(ctx)
}

func Test_TLog_FailOnFinalizeCommit(t *testing.T) {
	trans, _ := NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()

	b3, _ := NewBtree[PersonKey, Person](ctx, "tlogtable", nodeSlotLength, false, false, false, "", trans)

	pk, p := newPerson("joe", "shroeger", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.Commit(ctx)

	trans, _ = NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()

	b3, _ = OpenBtree[PersonKey, Person](ctx, "tlogtable", trans)
	pk, p = newPerson("joe", "shroeger", "male", "email2", "phone2")
	b3.Update(ctx, pk, p)

	synthesizeErrorOnFunction = finalizeCommit
	syntheticError = fmt.Errorf("SyntheticError")

	if err := trans.Commit(ctx); !strings.Contains(err.Error(), syntheticError.Error()) {
		t.Errorf("Commit failed, got %v, want %v.", err, syntheticError)
	}

	// TODO: add some code to cleanup tlogs...

}
