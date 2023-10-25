package btree

// Store contains a given (B-Tree) store details.
type Store struct {
	// Name of this (B-Tree store).
    Name string
	// Count of items that can be stored on a given node.
	NodeSlotCount int
	// IsUnique tells whether key/value pair (items) of this tree should be unique on key.
	IsUnique bool
	KeyInfo string
	ValueInfo string
	// RootNodeLogicalId is the root node's logical Id.
	RootNodeLogicalId UUID
	// Total count of items stored.
	Count int64
	// Version number.
	Version int
	// Is marked deleted or not.
	IsDeleted bool
}

// StoreInterface contains different repositories needed/used by B-Tree to manage/access its data/objects.
type StoreInterface[TKey Comparable, TValue any] struct{
	// StoreRepository is used to manage/access stores.
	StoreRepository StoreRepository
	// NodeRepository is used to manage/access B-Tree nodes.
	NodeRepository NodeRepository[TKey, TValue]
	// VirtualIdRepository is used to manage/access all objects keyed off of their virtual Ids (UUIDs).
	VirtualIdRepository VirtualIdRepository
	// RecyclerRepository is used to manage/access all deleted objects' "data blocks".
	RecyclerRepository RecyclerRepository
	// TransactionRepository is used to manage a transaction.
	TransactionRepository TransactionRepository
	// Transaction object if there is one.
	Transaction Transaction
}

// NewStore instantiates a new Store.
func NewStore(name string, nodeSlotCount int, isUnique bool) Store{
	return Store{
		Name: name,
		NodeSlotCount: nodeSlotCount,
		IsUnique: isUnique,
	}
}

type NodeDataBlocks struct {
	Id Handle
    SlotDataBlock []byte
	SlotDataBlockIds []UUID
	Children []UUID
	count int
	Version int
	IsDeleted bool
}

type SlotValue struct{
	Id Handle
	Value []byte
	IsDeleted bool
}

type SlotValueDataBlocks struct{
	Id Handle
	Value []byte
	ValueDataBlockIds []UUID
	IsDeleted bool
}

type Recyclable struct{
	ObjectType int
	ObjectId UUID
	LockDate int64
	IsDeleted bool
}

// VirtualId is a structure that holds Logical Id and the underlying current Physical Id it maps to.
// It also has other members used for Transaction processing.
type VirtualId struct {
	Handle
	IsDeleted bool
}

type TransactionActionType uint
const(
	Get = iota
	Add
	Update
	Remove
)

// TransactionEntry contain info about each Store Item modified within a Transaction.
// NOTE: newly created Stores themselves don't get tracked within the Transaction Entry table.
// Their items do. New Stores are cached in-memory and get saved (conflict resolved) 
// during Transaction Commit.
type TransactionEntry struct{
	Id Handle
	Sequence UUID
	Action TransactionActionType
	IsDeleted bool
}
