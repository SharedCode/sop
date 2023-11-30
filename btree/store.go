package btree

// Store contains a given (B-Tree) store details.
type Store struct {
	// Name of this (B-Tree store).
	Name string
	// Count of items that can be stored on a given node.
	SlotLength int
	// IsUnique tells whether key/value pair (items) of this tree should be unique on key.
	IsUnique  bool
	KeyInfo   string
	ValueInfo string
	// RootNodeId is the root node's Id.
	RootNodeId UUID
	// Total count of items stored.
	Count int64
	// Version number.
	Version int
	// Is marked deleted or not.
	IsDeleted bool
	// IsValueDataInNodeSegment is true if "Value" data is stored in the B-Tree node's data segment.
	// Otherwise is false.
	IsValueDataInNodeSegment bool
}

// StoreInterface contains different repositories needed/used by B-Tree to manage/access its data/objects.
type StoreInterface[TK Comparable, TV any] struct {
	// NodeRepository is used to manage/access B-Tree nodes.
	NodeRepository NodeRepository[TK, TV]
}

// NewStore instantiates a new Store.
func NewStore(name string, slotLength int, isUnique bool, isValueDataInNodeSegment bool) Store {
	// Only even numbered slot lengths are allowed as we reduced scenarios to simplify logic.
	if slotLength%2 != 0 {
		slotLength--
	}
	// Minimum slot length is 4, you lose gains if you use less than 4.
	if slotLength < 4 {
		slotLength = 4
	}
	return Store{
		Name:                     name,
		SlotLength:               slotLength,
		IsUnique:                 isUnique,
		IsValueDataInNodeSegment: isValueDataInNodeSegment,
	}
}

type NodeDataBlocks struct {
	Id               Handle
	SlotDataBlock    []byte
	SlotDataBlockIds []UUID
	Children         []UUID
	count            int
	Version          int
	IsDeleted        bool
}

type SlotValue struct {
	Id        Handle
	Value     []byte
	IsDeleted bool
}

type SlotValueDataBlocks struct {
	Id                Handle
	Value             []byte
	ValueDataBlockIds []UUID
	IsDeleted         bool
}

type Recyclable struct {
	ObjectType int
	ObjectId   UUID
	LockDate   int64
	IsDeleted  bool
}

type TransactionActionType uint

const (
	Get = iota
	Add
	Update
	Remove
)

type EntityType uint

const (
	// BTreeNode is the entity type of the B-Tree Node.
	BTreeNode = iota
	// ValuePart is the entity type of the value part in the key/value pair
	// that a B-Tree supports persistence & access.
	ValuePart
)

// TransactionEntry contain info about each Store Item modified within a Transaction.
// NOTE: newly created Stores themselves don't get tracked within the Transaction Entry table.
// Their items do. New Stores are cached in-memory and get saved (conflict resolved)
// during Transaction Commit.
type TransactionEntry struct {
	EntityLogicalId UUID
	EntityType      EntityType
	Sequence        int
	Action          TransactionActionType
	IsDeleted       bool
}
