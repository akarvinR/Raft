package raftshim

type IRaft interface {
	GetState() (int, bool)
	Kill()
	Snapshot(index int, snapshot []byte)
	Start(command interface{}) (int, int, bool)
}

type Callable interface {
	Call(method string, args interface{}, reply interface{}) bool
}

/*
func (*raft.Persister).Copy() *raft.Persister
func (*raft.Persister).RaftStateSize() int
func (*raft.Persister).ReadRaftState() []byte
func (*raft.Persister).ReadSnapshot() []byte
func (*raft.Persister).SaveRaftState(state []byte)
func (*raft.Persister).SaveStateAndSnapshot(state []byte, snapshot []byte)
func (*raft.Persister).SnapshotSize() int
*/

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Persistable interface {
	Copy() Persistable
	RaftStateSize() int
	ReadRaftState() []byte
	ReadSnapshot() []byte
	SaveRaftState(state []byte)
	SaveStateAndSnapshot(state []byte, snapshot []byte)
	SnapshotSize() int
}
