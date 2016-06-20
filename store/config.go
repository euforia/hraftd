package store

const (
	DefaultRaftAddr    = ":12000"
	DefaultRaftDataDir = ".hraft"
)

type HraftConfig struct {
	EnableRaftLogging   bool   // raft specific logging.  low-level
	EnableDebug         bool   // non raft specific logs
	EnableLeaderForward bool   // forward messages to leader if node is not leader
	RaftDataDir         string // place for raft to store data
	RaftBindAddr        string // raft address to listen on
	JoinAddr            string // initial nodes to join
}

func DefaultHraftConfig() HraftConfig {
	return HraftConfig{
		EnableLeaderForward: true,
		RaftBindAddr:        DefaultRaftAddr,
		RaftDataDir:         DefaultRaftDataDir,
	}
}
