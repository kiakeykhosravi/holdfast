package types

import "time"

type DecryptResult struct {
	Mode         string    `json:"mode"`
	Input        string    `json:"input"`
	Output       string    `json:"output"`
	ChecksumFile string    `json:"checksumFile,omitempty"`
	ChecksumOK   *bool     `json:"checksumOK,omitempty"`
	BytesWritten int64     `json:"bytesWritten"`
	CreatedAt    time.Time `json:"createdAt"`
}

type ValidateOnlyResult struct {
	Mode         string `json:"mode"`
	Input        string `json:"input"`
	ChecksumFile string `json:"checksumFile,omitempty"`
	ChecksumOK   *bool  `json:"checksumOK,omitempty"`
	DockerImage  string `json:"dockerImage"`

	// ChainMembers is how many backups had to be restored together. 1 for a
	// logical dump or a physical full; more for an incremental.
	ChainMembers int `json:"chainMembers"`

	// MetaRecorded reports whether the verdict was written to a sidecar. False
	// when the archive has none, in which case parent resolution still will not
	// treat this backup as proven.
	MetaRecorded bool `json:"metaRecorded"`

	TestedOK  bool      `json:"testedOK"`
	CreatedAt time.Time `json:"createdAt"`
}

type RetentionResult struct {
	Mode string `json:"mode"`
	// Storage is where retention looked: a directory or an s3:// URL.
	Storage        string `json:"storage"`
	RetentionDays  int    `json:"retentionDays"`
	MinChainsKept  int    `json:"minChainsKept"`
	ChainsDeleted  int    `json:"chainsDeleted"`
	BackupsDeleted int    `json:"backupsDeleted"`
	FilesDeleted   int    `json:"filesDeleted"`
	ChainsKept     int    `json:"chainsKept"`
	// ChainsKeptByFloor counts chains that had aged out but were kept because
	// they are among the min_chains_kept newest.
	ChainsKeptByFloor int       `json:"chainsKeptByFloor"`
	OrphansDeleted    int       `json:"orphansDeleted"`
	CreatedAt         time.Time `json:"createdAt"`
}

// SyncResult is the JSON printed after -sync-mirrors: one entry per source
// synced in this run, and the total that failed.
type SyncResult struct {
	Mode    string             `json:"mode"`
	Sources []SourceSyncResult `json:"sources"`

	// Failed is the total across sources and mirrors. Non-zero means some
	// mirror is still behind; main exits 3.
	Failed int `json:"failed"`

	CreatedAt time.Time `json:"createdAt"`
}

// SourceSyncResult is one source's part of a sync run.
type SourceSyncResult struct {
	// Name is the configured source name, empty for the implicit single
	// source of a config without a sources block.
	Name    string             `json:"name,omitempty"`
	Primary string             `json:"primary"`
	Mirrors []MirrorSyncResult `json:"mirrors"`
	Failed  int                `json:"failed"`

	// PruneRefused explains why -prune was ignored for this source, empty
	// otherwise.
	PruneRefused string `json:"pruneRefused,omitempty"`

	// Error is set when the source could not be synced at all (its primary
	// could not be listed, its stores could not be built).
	Error string `json:"error,omitempty"`
}

type MirrorSyncResult struct {
	Mirror      string   `json:"mirror"`
	Copied      int      `json:"copied"`
	CopiedBytes int64    `json:"copiedBytes"`
	UpToDate    int      `json:"upToDate"`
	Pruned      int      `json:"pruned"`
	Failed      []string `json:"failed,omitempty"`
}
