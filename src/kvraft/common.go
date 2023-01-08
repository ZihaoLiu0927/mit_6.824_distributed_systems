package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRaftNoRes   = "ErrRaftNoRes"
	GET            = "GET"
	PUT            = "PUT"
	APPEND         = "APPEND"
	EmptyValue     = ""
)

type Err string

// Put or Append
// Field names must start with capital letters,
// otherwise RPC will break.
type PutAppendArgs struct {
	Key          string
	Value        string
	Op           string
	Opid         int
	Client       int64
	LastSeenOpid int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key          string
	Opid         int
	Client       int64
	LastSeenOpid int
}

type GetReply struct {
	Err   Err
	Value string
}

type ServerState struct {
	Data   map[string]string
	Record map[int64]map[int]recordRes
}
