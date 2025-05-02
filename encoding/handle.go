package encoding

import (
	"bytes"
	"encoding/binary"

	"github.com/google/uuid"

	"github.com/SharedCode/sop"
)

type HandleEncoder struct{}

// Instantiates a Handler Marshaler.
func NewHandleMarshaler() *HandleEncoder {
	return &HandleEncoder{}
}

// Encodes handler to byte array.
func (he HandleEncoder) Marshal(v any) ([]byte, error) {
	w := bytes.NewBuffer(make([]byte, 0, sop.HandleSizeInBytes))
	pv := v.(sop.Handle)
	encode(w, &pv)
	return w.Bytes(), nil
}

// Decodes byte array back to a handler type.
func (he HandleEncoder) Unmarshal(data []byte, v any) error {
	r := bytes.NewBuffer(data)
	h, err := decode(r)
	target := v.(*sop.Handle)
	*target = *h
	return err
}

func (he HandleEncoder) UnmarshalLogicalID(data []byte) (sop.UUID, error) {
	r := bytes.NewBuffer(data)
	h, err := uuid.FromBytes(r.Next(16))
	if err != nil {
		return sop.NilUUID, err
	}
	return sop.UUID(h), nil
}

func encode(w *bytes.Buffer, h *sop.Handle) (int, error) {
	w.Write(h.LogicalID[:])
	w.Write(h.PhysicalIDA[:])
	w.Write(h.PhysicalIDB[:])
	var b byte
	if h.IsActiveIDB {
		b = 1
	}
	w.Write([]byte{b})

	var dummy4 [4]byte
	binary.LittleEndian.PutUint32(dummy4[:], uint32(h.Version))
	w.Write(dummy4[:])

	var dummy8 [8]byte
	binary.LittleEndian.PutUint64(dummy8[:], uint64(h.WorkInProgressTimestamp))
	w.Write(dummy8[:])

	b = 0
	if h.IsDeleted {
		b = 1
	}
	w.Write([]byte{b})

	return w.Len(), nil
}

func decode(r *bytes.Buffer) (*sop.Handle, error) {
	var result sop.Handle
	h, err := uuid.FromBytes(r.Next(16))
	if err != nil {
		return &result, err
	}
	result.LogicalID = sop.UUID(h)

	h, err = uuid.FromBytes(r.Next(16))
	if err != nil {
		return &result, err
	}
	result.PhysicalIDA = sop.UUID(h)

	h, err = uuid.FromBytes(r.Next(16))
	if err != nil {
		return &result, err
	}
	result.PhysicalIDB = sop.UUID(h)

	var b byte = r.Next(1)[0]
	if b == 1 {
		result.IsActiveIDB = true
	}

	result.Version = int32(binary.LittleEndian.Uint32(r.Next(4)))
	result.WorkInProgressTimestamp = int64(binary.LittleEndian.Uint64(r.Next(8)))

	b = r.Next(1)[0]
	if b == 1 {
		result.IsDeleted = true
	}

	return &result, nil
}
