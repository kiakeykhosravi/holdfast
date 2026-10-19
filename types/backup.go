package types

import (
	"fmt"
	"strings"
	"time"
)

type ParentRef struct {
	ID        string     `json:"id"`
	Type      BackupType `json:"type"`
	ChainID   string     `json:"chainID"`
	StatePath string     `json:"statePath"`

	// Timeline is the engine's timeline/branch identifier for the parent, or 0
	// when unknown (a sidecar written before this field existed). A chain may
	// not span a timeline change: after a failover or PITR the history has
	// forked, and combining backups from either side of the fork produces a
	// cluster that is not any point in time the database ever occupied.
	Timeline int `json:"timeline,omitempty"`
}

type BackupRequest struct {
	Type BackupType `json:"type"`

	// Parent is nil for any full backup and required for incrementals.
	Parent *ParentRef `json:"parent,omitempty"`
}

func (r BackupRequest) Validate() error {
	if err := r.Type.Validate(); err != nil {
		return err
	}
	if r.Type.RequiresParent() {
		if r.Parent == nil {
			return fmt.Errorf("backup type %s requires a parent", r.Type)
		}
		if strings.TrimSpace(r.Parent.ID) == "" {
			return fmt.Errorf("parent reference is missing an ID")
		}
		if strings.TrimSpace(r.Parent.StatePath) == "" {
			return fmt.Errorf("parent %s is missing a state path", r.Parent.ID)
		}
		if err := r.Parent.Type.Validate(); err != nil {
			return fmt.Errorf("parent %s: %w", r.Parent.ID, err)
		}
		return nil
	}
	if r.Parent != nil {
		return fmt.Errorf("backup type %s must not specify a parent", r.Type)
	}
	return nil
}

type DumpOutcome struct {
	Entries       []string `json:"entries"`
	State         []byte   `json:"-"`
	PositionStart string   `json:"positionStart,omitempty"`
	PositionEnd   string   `json:"positionEnd,omitempty"`

	// Timeline is the engine's timeline/branch identifier, 0 if the driver has
	// no such concept or could not determine it.
	Timeline int `json:"timeline,omitempty"`
}

type BackupResult struct {
	ID                string     `json:"id"`
	DBType            string     `json:"dbType"`
	BackupType        BackupType `json:"backupType,omitzero"`
	ParentID          string     `json:"parentID,omitempty"`
	ChainID           string     `json:"chainID,omitempty"`
	EncryptedFileName string     `json:"encryptedFileName"`
	ChecksumFile      string     `json:"checksumFile"`
	BackupSizeBytes   int64      `json:"backupSizeBytes"`
	CreatedAt         time.Time  `json:"createdAt"`
	TestedOK          bool       `json:"testedOK"`
}

// IncrementalUnavailableError means an incremental cannot be taken right now,
// but a full backup would succeed. The server aged out the WAL summaries the
// parent depends on, or a standby has not produced a restartpoint since the
// last backup.
//
// This is not a failure: the correct response is to take a full backup, which
// api does when fallback_to_full is set. It is a distinct type rather than a
// bool so the reason survives into the logs.
type IncrementalUnavailableError struct {
	Reason string
}

func (e *IncrementalUnavailableError) Error() string {
	return "incremental backup unavailable: " + e.Reason
}
