package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"holdfast/interfaces"
	"holdfast/types"
)

// DryRunResult is the JSON output of a -dry-run invocation.
type DryRunResult struct {
	Mode         string            `json:"mode"`
	ConfigOK     bool              `json:"configOK"`
	ConfigErrors map[string]string `json:"configErrors,omitempty"`

	// Storage is where backups would go: a directory or an s3:// URL.
	// StorageOK means a probe object could be written and deleted there.
	Storage    string `json:"storage,omitempty"`
	StorageOK  bool   `json:"storageOK"`
	StorageErr string `json:"storageErr,omitempty"`

	ScratchDirOK     bool   `json:"scratchDirOK"`
	ScratchDirErr    string `json:"scratchDirErr,omitempty"`
	ScratchFreeBytes int64  `json:"scratchFreeBytes,omitempty"`
	ScratchSpaceNote string `json:"scratchSpaceNote,omitempty"`

	// LockAvailable is true when the run lock could be taken. LockNote
	// explains why not: either another run holds it (not a fault) or it
	// cannot be taken at all (a fault, also recorded in ConfigErrors).
	LockAvailable bool   `json:"lockAvailable"`
	LockNote      string `json:"lockNote,omitempty"`

	// PlannedType is the type that would actually be produced, which differs
	// from the requested type when a fallback would apply.
	PlannedType       types.BackupType `json:"plannedType,omitzero"`
	RequestedType     types.BackupType `json:"requestedType,omitzero"`
	PlannedParentID   string           `json:"plannedParentID,omitempty"`
	FallbackReason    string           `json:"fallbackReason,omitempty"`
	ParentStateCached *bool            `json:"parentStateCached,omitempty"`

	ShouldRun     bool   `json:"shouldRun"`
	ShouldRunNote string `json:"shouldRunNote,omitempty"`
	ShouldRunErr  string `json:"shouldRunErr,omitempty"`

	CreatedAt time.Time `json:"createdAt"`
}

// RunDryRun validates config, checks the directories and the run lock, works
// out which backup would actually be taken, and runs the preflight gate —
// without writing any backup data.
//
// The result is always returned, even alongside an error, so main can print
// the full diagnosis rather than a one-line summary of it.
//
// # Why this does its own parent planning
//
// It must not materialise the parent's state. That decrypts the entire parent
// archive and writes to state_dir when the cache is cold — a dry run that
// writes files and takes an hour on a large cluster is not usable as the
// pre-deployment check it exists to be. SelectParentMeta reads sidecars only,
// which is all a dry run needs to report the plan.
//
// store may be nil when it could not be constructed; storeErr then says why,
// and the storage-dependent steps are reported as not run rather than
// skipped silently.
func RunDryRun(
	ctx context.Context,
	cfg types.Config,
	opts types.CLIOptions,
	store interfaces.Storer,
	storeErr error,
	backupDB interfaces.BackupDB,
	encryptor interfaces.Encryptor,
) (*DryRunResult, error) {

	result := &DryRunResult{
		Mode:          string(types.RunModeDryRun),
		RequestedType: cfg.BackupType,
		CreatedAt:     time.Now(),
	}
	addConfigErr := func(k, v string) {
		result.ConfigOK = false
		if result.ConfigErrors == nil {
			result.ConfigErrors = map[string]string{}
		}
		result.ConfigErrors[k] = v
	}

	// 1. Config validation (same rules as a real backup run).
	if errs := cfg.ValidateForMode(types.CLIOptions{Mode: types.RunModeBackup}); len(errs) > 0 {
		result.ConfigOK = false
		result.ConfigErrors = errs
	} else {
		result.ConfigOK = true
	}

	// 2. Storage — reachable and writable. A probe object is written through
	// the same Create/Commit path a real archive takes, then deleted.
	switch {
	case storeErr != nil:
		result.StorageErr = fmt.Sprintf("storage unavailable: %v", storeErr)
		addConfigErr("Storage", storeErr.Error())
	default:
		result.Storage = store.Location()
		if err := probeStorage(ctx, store); err != nil {
			result.StorageErr = fmt.Sprintf("%s is not writable: %v", store.Location(), err)
		} else {
			result.StorageOK = true
		}
	}

	// 3. Scratch directory — writable, and large enough to be plausible.
	// Physical backups stage a full copy of the cluster here before anything
	// reaches the archive, and chain validation needs several more.
	scratch := cfg.ScratchDir
	if scratch == "" {
		scratch = os.TempDir()
		result.ScratchSpaceNote = fmt.Sprintf("scratch_dir is unset, so %s would be used; on most hosts that is tmpfs (RAM)", scratch)
	}
	if err := os.MkdirAll(scratch, 0o750); err != nil {
		result.ScratchDirErr = fmt.Sprintf("cannot create scratch_dir %q: %v", scratch, err)
	} else if err := probeWritable(scratch); err != nil {
		result.ScratchDirErr = fmt.Sprintf("scratch_dir %q is not writable: %v", scratch, err)
	} else {
		result.ScratchDirOK = true
		if free, err := FreeBytes(scratch); err == nil {
			result.ScratchFreeBytes = free
		}
	}

	// 4. Run lock. Contention is informational; anything else is the fault
	// that used to be reported as a healthy skip.
	if unlock, err := AcquireRunLock(LockDir(cfg)); err != nil {
		var held *LockHeldError
		if errors.As(err, &held) {
			result.LockNote = held.Error()
		} else {
			result.LockNote = err.Error()
			addConfigErr("RunLock", err.Error())
		}
	} else {
		unlock()
		result.LockAvailable = true
	}

	// 5. What would actually be backed up. Needs the store for the sidecars.
	req := types.BackupRequest{Type: cfg.BackupType}
	var (
		meta      types.BackupMeta
		hasParent bool
		perr      error
	)
	if store != nil {
		meta, hasParent, perr = SelectParentMeta(ctx, cfg, store, cfg.BackupType, opts.ParentID)
	} else if cfg.BackupType.RequiresParent() {
		perr = fmt.Errorf("cannot plan a parent without storage")
	}
	if perr != nil {
		var unavailable *types.IncrementalUnavailableError
		if errors.As(perr, &unavailable) && cfg.FallbackToFull && cfg.BackupType.RequiresParent() {
			result.FallbackReason = unavailable.Reason
			req = types.BackupRequest{Type: types.BackupType{Medium: cfg.BackupType.Medium, Level: types.LevelFull}}
		} else {
			addConfigErr("Parent", perr.Error())
		}
	} else if hasParent {
		req.Parent = ParentRefFor(cfg, meta)
		_, statErr := os.Stat(req.Parent.StatePath)
		cached := statErr == nil
		result.ParentStateCached = &cached
		result.PlannedParentID = meta.ID
	}
	result.PlannedType = req.Type

	if req.Type.Medium == types.MediumPhysical && result.ScratchFreeBytes > 0 {
		result.ScratchSpaceNote = appendNote(result.ScratchSpaceNote,
			"a physical backup stages a full copy of the cluster in scratch_dir, and validating a chain of N members needs roughly (2N+1) further copies")
	}

	// 6. Engine rules and the preflight gate.
	if err := backupDB.ValidateRequest(req); err != nil {
		addConfigErr("BackupType", err.Error())
	}

	ok, note, err := backupDB.ShouldRun(ctx, req)
	if err != nil {
		result.ShouldRun = false
		result.ShouldRunErr = err.Error()
	} else {
		result.ShouldRun = ok
		result.ShouldRunNote = note
	}

	// Surface an error only if the config itself is broken — callers should
	// still receive the result struct for partial diagnostics.
	if !result.ConfigOK {
		return result, fmt.Errorf("dry-run: config validation failed")
	}
	return result, nil
}

func probeWritable(dir string) error {
	probe := filepath.Join(dir, ".dry_run_probe")
	if err := os.WriteFile(probe, []byte("probe"), 0o600); err != nil {
		return err
	}
	return os.Remove(probe)
}

// probeStorage writes and removes a small object. The name starts with "."
// so the file backend's List and retention's name scans never see it, even
// if the delete fails.
func probeStorage(ctx context.Context, store interfaces.Storer) error {
	const probe = ".dry_run_probe"
	if err := interfaces.PutObject(ctx, store, probe, []byte("probe")); err != nil {
		return err
	}
	return store.Delete(ctx, probe)
}

func appendNote(existing, add string) string {
	if existing == "" {
		return add
	}
	return existing + "; " + add
}
