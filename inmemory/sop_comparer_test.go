package inmemory

import (
	"cmp"
	"testing"
)

type personKey struct {
	firstname string
	lastname  string
}

type person struct {
	personKey
	gender string
	email  string
	phone  string
}

func newPerson(fname string, lname string, gender string, email string, phone string) person {
	return person{
		personKey: personKey{
			firstname: fname,
			lastname:  lname,
		},
		gender: gender,
		email:  email,
		phone:  phone,
	}
}
func (x personKey) Compare(other interface{}) int {
	y := other.(personKey)
	i := cmp.Compare[string](x.lastname, y.lastname)
	if i != 0 {
		return i
	}
	return cmp.Compare[string](x.firstname, y.firstname)
}

func Test_PersonLookup(t *testing.T) {
	t.Log("Btree demo, used as a person struct lookup.\n")

	p := newPerson("joe", "krueger", "male", "email", "phone")
	b3 := NewBtree[personKey, person](false)
	b3.Add(p.personKey, p)

	if b3.Find(p.personKey, false) {
		t.Logf("Person w/ key %v found.", b3.GetCurrentKey())
	}

	if !passBtreeAround(b3) {
		t.Logf("passBtreeAround(b3) failed, got false, want true.")
	}

	t.Log("Btree demo, used as a person struct lookup end.\n")
}

func passBtreeAround(b3 BtreeInterface[personKey, person]) bool {
	key := personKey{firstname: "joe", lastname: "krueger"}
	return b3.Find(key, false)
}

func Test_TypesInCompare(t *testing.T) {
	b3Int := NewBtree[int, int](true)
	b3Int.Add(1, 1)
	b3Int.Find(1, false)

	b3Int8 := NewBtree[int8, int8](true)
	b3Int8.Add(1, 1)
	b3Int8.Find(1, false)

	b3Int16 := NewBtree[int16, int16](true)
	b3Int16.Add(1, 1)
	b3Int16.Find(1, false)

	b3Int32 := NewBtree[int32, int32](true)
	b3Int32.Add(1, 1)
	b3Int32.Find(1, false)

	b3Int64 := NewBtree[int64, int64](true)
	b3Int64.Add(1, 1)
	b3Int64.Find(1, false)

	b3UInt := NewBtree[uint, uint](true)
	b3UInt.Add(1, 1)
	b3UInt.Find(1, false)

	b3UInt8 := NewBtree[uint8, uint8](true)
	b3UInt8.Add(1, 1)
	b3UInt8.Find(1, false)

	b3UInt16 := NewBtree[uint16, uint16](true)
	b3UInt16.Add(1, 1)
	b3UInt16.Find(1, false)

	b3UInt32 := NewBtree[uint32, uint32](true)
	b3UInt32.Add(1, 1)
	b3UInt32.Find(1, false)

	b3UInt64 := NewBtree[uint64, uint64](true)
	b3UInt64.Add(1, 1)
	b3UInt64.Find(1, false)

	b3uintptr := NewBtree[uintptr, uintptr](true)
	b3uintptr.Add(1, 1)
	b3uintptr.Find(1, false)

	b3float32 := NewBtree[float32, float32](true)
	b3float32.Add(1, 1)
	b3float32.Find(1, false)

	b3float64 := NewBtree[float64, float64](true)
	b3float64.Add(1, 1)
	b3float64.Find(1, false)

	b3str := NewBtree[string, string](true)
	b3str.Add("1", "1")
	b3str.Find("1", false)
}
