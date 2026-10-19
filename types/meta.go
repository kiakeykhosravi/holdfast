package types

import (
	"fmt"
	"strings"
	"time"
)

// MetaSchemaVersion is bumped whenever the sidecar format changes in a way
// older readers cannot handle. Readers must refuse versions they do not know
// rather than guessing: a retention pass that misreads a chain deletes data.
const MetaSchemaVersion = 1

// ValidationResult records the outcome of the last restore test.
type ValidationResult string

const (
	ValidationNotRun ValidationResult = ""
	ValidationOK     ValidationResult = "ok"
	ValidationFailed ValidationResult = "failed"
)

// BackupMeta is the unencrypted sidecar written next to every archive, as
// backup_<id>.meta.json.
//
// Its purpose is to make a chain reconstructible from the backup directory
// alone — no database connection, no external index, no passphrase. That
// matters because the directory may be all that survives, and because
// retention has to run on a recovery host at 3am with the primary down.
//
// # What must never go in here
//
// The file is plaintext. Anything in it is readable by whoever can list the
// backup directory. Database names, table names, schema details, and file
// paths from inside the cluster all stay in the encrypted archive. This is why
// there is no Entries field: for a logical backup the zip entries are
// "<dbname>.dump" for every database in the cluster, which is a schema
// disclosure in exchange for information nothing outside validation needs.
// Restore order is recoverable from the zip's own directory once decrypted.
type BackupMeta struct {
	SchemaVersion int `json:"schemaVersion"`

	// ID is the timestamp identifier, e.g. "2026-08-25_03-00-00". Unique per
	// backup and used to derive every filename in the set.
	ID string `json:"id"`

	// DBType is the driver name, e.g. "postgres".
	DBType string `json:"dbType"`

	Type BackupType `json:"type"`

	// ChainID is the ID of the full at the root of this backup's chain. A full
	// has ChainID == ID. Retention groups on this field.
	ChainID string `json:"chainID"`

	// ParentID is the immediate predecessor, empty for a full. Restore walks
	// these links; retention uses them to refuse deleting an ancestor while a
	// descendant is retained.
	ParentID string `json:"parentID,omitempty"`

	CreatedAt time.Time `json:"createdAt"`

	// EncryptedFile is the basename of the archive, not a full path, so a
	// backup set stays valid when the directory is moved or restored elsewhere.
	EncryptedFile string `json:"encryptedFile"`

	SizeBytes      int64  `json:"sizeBytes"`
	ChecksumSHA256 string `json:"checksumSHA256"`

	// EntryCount is how many files the archive holds. Enough for a sanity check
	// at restore time without naming any of them.
	EntryCount int `json:"entryCount"`

	// Encryption records the container format so a future format change can be
	// detected before attempting a decrypt.
	Encryption EncryptionInfo `json:"encryption"`

	// PositionStart and PositionEnd bound the change window in the engine's own
	// notation — LSNs, GTIDs, oplog timestamps. Opaque outside the driver.
	PositionStart string `json:"positionStart,omitempty"`
	PositionEnd   string `json:"positionEnd,omitempty"`

	// Timeline is the engine's timeline/branch identifier, 0 when unknown.
	//
	// Adding this did not need a MetaSchemaVersion bump: encoding/json ignores
	// unknown fields, so an older build reads a newer sidecar fine, and a newer
	// build reading an older sidecar gets 0, which every check below treats as
	// "unknown, do not enforce".
	Timeline int `json:"timeline,omitempty"`

	ValidatedAt      *time.Time       `json:"validatedAt,omitempty"`
	ValidationResult ValidationResult `json:"validationResult,omitempty"`

	// Signature is an HMAC-SHA256, hex encoded, over the fields above, keyed
	// by a value derived from the backup passphrase. It lets a reader that
	// knows the passphrase detect a sidecar that was edited in place. Empty on
	// sidecars written before signing existed; those are accepted with a
	// warning. See api.MetaKey.
	Signature string `json:"signature,omitempty"`
}

type EncryptionInfo struct {
	// Container is the Encryptor's Name(), e.g. "aes-ctr-hmac". It identifies
	// the on-disk container format so a future format change can be detected
	// before attempting a decrypt. The magic bytes inside the file ("DBBK1")
	// are the encryptor's own concern.
	Container string `json:"container"`
	KDF       string `json:"kdf"`
}

// MetaFileName is the sidecar filename for a backup ID.
func MetaFileName(id string) string { return "backup_" + id + ".meta.json" }

// IsChainRoot reports whether this backup starts a chain — nothing precedes it.
func (m BackupMeta) IsChainRoot() bool { return strings.TrimSpace(m.ParentID) == "" }

// ValidatedOK reports whether the last restore test on this backup passed.
// Parent resolution uses this: chaining an incremental onto a backup that has
// never been proven restorable compounds a problem instead of catching it.
func (m BackupMeta) ValidatedOK() bool { return m.ValidationResult == ValidationOK }

// Validate checks the sidecar is internally coherent. A file that fails this
// must be treated as "unknown dependencies, do not touch" rather than skipped
// silently.
func (m BackupMeta) Validate() error {
	if m.SchemaVersion == 0 {
		return fmt.Errorf("missing schemaVersion")
	}
	if m.SchemaVersion > MetaSchemaVersion {
		return fmt.Errorf("schemaVersion %d is newer than this build understands (max %d)", m.SchemaVersion, MetaSchemaVersion)
	}
	if strings.TrimSpace(m.ID) == "" {
		return fmt.Errorf("missing id")
	}
	if err := m.Type.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(m.ChainID) == "" {
		return fmt.Errorf("missing chainID")
	}
	if strings.TrimSpace(m.EncryptedFile) == "" {
		return fmt.Errorf("missing encryptedFile")
	}
	if m.CreatedAt.IsZero() {
		return fmt.Errorf("missing createdAt")
	}
	if m.IsChainRoot() {
		if !m.Type.IsFull() {
			return fmt.Errorf("%s has no parent but is not a full backup", m.ID)
		}
		if m.ChainID != m.ID {
			return fmt.Errorf("%s is a chain root but chainID is %q", m.ID, m.ChainID)
		}
		return nil
	}
	if m.Type.IsFull() {
		return fmt.Errorf("%s is a full backup but declares parent %q", m.ID, m.ParentID)
	}
	if m.ChainID == m.ID {
		return fmt.Errorf("%s has parent %q but claims to be its own chain root", m.ID, m.ParentID)
	}
	return nil
}

// ToResult projects the sidecar into the JSON printed on stdout after a run.
func (m BackupMeta) ToResult(encryptedPath, checksumPath string) *BackupResult {
	return &BackupResult{
		ID:                m.ID,
		DBType:            m.DBType,
		BackupType:        m.Type,
		ParentID:          m.ParentID,
		ChainID:           m.ChainID,
		EncryptedFileName: encryptedPath,
		ChecksumFile:      checksumPath,
		BackupSizeBytes:   m.SizeBytes,
		CreatedAt:         m.CreatedAt,
		TestedOK:          m.ValidatedOK(),
	}
}

type ExtractedBackup struct {
	Meta  BackupMeta
	Files []string
}
