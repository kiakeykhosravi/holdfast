package api

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"holdfast/interfaces"
	"holdfast/types"
)

// SkipError means preflight said not to run — a lagging replica, a node in the
// wrong cluster role. That is the tool working correctly, so main exits 0 on
// it. Returning a plain error here is why a healthy skip used to page someone.
type SkipError struct {
	Reason string
}

func (e *SkipError) Error() string { return "skipped: " + e.Reason }

// SkipResult is printed on stdout when a run is skipped.
type SkipResult struct {
	Mode      string    `json:"mode"`
	Skipped   bool      `json:"skipped"`
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"createdAt"`
}

// archiveArtifact is what writeEncryptedArchive committed to the store.
type archiveArtifact struct {
	ID           string
	FileName     string // archive object name
	ChecksumName string // checksum object name
	ChecksumHex  string
	SizeBytes    int64
	StartedAt    time.Time
}

// fallbackToFull returns the full-backup form of req when err means "an
// incremental is impossible right now" and the config permits the downgrade.
//
// A missed incremental leaves the run with no backup at all, which is strictly
// worse than an unexpectedly large one. Centralising the decision here is what
// makes fallback_to_full mean the same thing at each of the four points it can
// be needed: parent selection, preflight, parent state materialisation, and
// the dump itself.
func fallbackToFull(cfg types.Config, req types.BackupRequest, err error) (types.BackupRequest, bool) {
	var unavailable *types.IncrementalUnavailableError
	if !errors.As(err, &unavailable) {
		return req, false
	}
	if !cfg.FallbackToFull || !req.Type.RequiresParent() {
		return req, false
	}
	log.Printf("WARNING: %v; falling back to a full backup", unavailable)
	return types.BackupRequest{Type: types.BackupType{Medium: req.Type.Medium, Level: types.LevelFull}}, true
}

// acquireOrSkip takes the run lock and classifies the outcome.
//
// Contention is a skip: the previous run is still working. Contention that
// has lasted longer than a run could legitimately take is not — it means the
// previous run is stuck, and reporting that as a healthy skip would let a
// host go without backups indefinitely while every run exited 0. Any other
// failure (state_dir not creatable, lock file not openable) is a fault.
func acquireOrSkip(cfg types.Config) (func(), error) {
	unlock, err := AcquireRunLock(LockDir(cfg))
	if err == nil {
		return unlock, nil
	}
	var held *LockHeldError
	if !errors.As(err, &held) {
		return nil, fmt.Errorf("cannot take the run lock: %w", err)
	}
	if limit := cfg.DumpTimeout + cfg.RestoreTimeout; limit > 0 && held.HeldFor > limit {
		return nil, fmt.Errorf("%v, longer than dump_timeout + restore_timeout (%s); the previous run is probably stuck", held, limit)
	}
	return nil, &SkipError{Reason: held.Error()}
}

// StartBackup orchestrates a backup run. Everything type-specific is delegated:
// this function decides the sequence, not the mechanics.
//
// Parent resolution happens here rather than in main so that every way an
// incremental can prove impossible converges on one place that knows how to
// downgrade it: no backups yet, no backup of this medium, a chain at its length
// limit, a tip that failed its restore test, a chain member whose archive is
// gone, aged-out WAL summaries, a timeline change, a parent archive that will
// not decrypt, or the engine refusing mid-dump. Resolving in main meant the
// first four exited 2 with no backup taken, every run, until a human
// intervened.
func StartBackup(
	ctx context.Context,
	cfg types.Config,
	store interfaces.Storer,
	backupDB interfaces.BackupDB,
	encryptor interfaces.Encryptor,
	requested types.BackupType,
	explicitParentID string,
) (*types.BackupResult, error) {

	// Held for the whole run, validation included, so a schedule firing faster
	// than the backup completes cannot interleave two runs' chain bookkeeping.
	unlock, err := acquireOrSkip(cfg)
	if err != nil {
		return nil, err
	}
	defer unlock()

	// Safe only now: the lock proves no other run on this host owns anything
	// the sweep might remove.
	SweepStaleTemps(ctx, cfg, store)

	key := metaKeyFor(cfg)

	// Sign any sidecar that predates signing. Done here, under the lock and
	// with the passphrase available, so that one backup run per host is the
	// whole migration to require_signed_sidecars. Skipped once that flag is
	// on, since an unsigned sidecar is then untrusted and must not be
	// laundered into a signed one.
	if key != nil && !cfg.RequireSignedSidecars {
		if n, err := SignUnsignedSidecars(ctx, store, key); err != nil {
			log.Printf("WARNING: could not sign legacy sidecars in %s: %v", store.Location(), err)
		} else if n > 0 {
			log.Printf("[meta] signed %d legacy sidecar(s) in %s; set require_signed_sidecars: true once every host has run this version", n, store.Location())
		}
	}

	// 1. Which backup, and on which parent. Sidecars only; the parent archive
	// is not opened until preflight has agreed this node should run at all.
	req := types.BackupRequest{Type: requested}
	var parentMeta types.BackupMeta
	meta, hasParent, err := SelectParentMeta(ctx, cfg, store, requested, explicitParentID)
	if err != nil {
		downgraded, isFallback := fallbackToFull(cfg, req, err)
		if !isFallback {
			return nil, err
		}
		req = downgraded
	} else if hasParent {
		parentMeta = meta
		req.Parent = ParentRefFor(cfg, meta)
	}

	// 2. Engine rules and the preflight gate.
	if err := backupDB.ValidateRequest(req); err != nil {
		return nil, err
	}
	ok, reason, err := backupDB.ShouldRun(ctx, req)
	if err != nil {
		if downgraded, isFallback := fallbackToFull(cfg, req, err); isFallback {
			req = downgraded
			if verr := backupDB.ValidateRequest(req); verr != nil {
				return nil, verr
			}
			ok, reason, err = backupDB.ShouldRun(ctx, req)
		}
		if err != nil {
			return nil, fmt.Errorf("pre-flight check failed: %w", err)
		}
	}
	if !ok {
		return nil, &SkipError{Reason: reason}
	}

	// 3. The parent's driver state. Deferred until here because on a cold
	// cache it decrypts an entire archive, and preflight may have just said
	// this node should not run at all.
	if req.Parent != nil {
		if err := MaterialiseParentState(ctx, cfg, store, encryptor, parentMeta); err != nil {
			downgraded, isFallback := fallbackToFull(cfg, req, err)
			if !isFallback {
				return nil, err
			}
			req = downgraded
			if verr := backupDB.ValidateRequest(req); verr != nil {
				return nil, verr
			}
			ok, reason, err = backupDB.ShouldRun(ctx, req)
			if err != nil {
				return nil, fmt.Errorf("pre-flight check failed: %w", err)
			}
			if !ok {
				return nil, &SkipError{Reason: reason}
			}
		}
	}

	// 4. The dump.
	art, outcome, err := writeEncryptedArchive(ctx, cfg, store, backupDB, encryptor, req)
	if err != nil {
		// pg_basebackup can reject an incremental that passed preflight: the
		// summary-coverage check is best-effort and gives up silently when it
		// cannot parse the parent manifest, so this is the path that actually
		// fires in production. Previously this error was returned unconditionally
		// and fallback_to_full never saw it.
		if downgraded, isFallback := fallbackToFull(cfg, req, err); isFallback {
			req = downgraded
			if verr := backupDB.ValidateRequest(req); verr != nil {
				return nil, verr
			}
			art, outcome, err = writeEncryptedArchive(ctx, cfg, store, backupDB, encryptor, req)
		}
		if err != nil {
			return nil, err
		}
	}

	// Cache the driver state before anything else can fail: without it the next
	// incremental has to decrypt this archive to find its manifest.
	if len(outcome.State) > 0 {
		if serr := WriteState(cfg, art.ID, outcome.State); serr != nil {
			log.Printf("WARNING: could not cache parent state for %s: %v", art.ID, serr)
		}
	}

	meta = buildMeta(backupDB.Name(), encryptor.Name(), req, art, outcome)
	if err := WriteMeta(ctx, store, meta, key); err != nil {
		return nil, fmt.Errorf("failed to write backup metadata: %w", err)
	}

	if cfg.EnableRestoreTest {
		validateErr := validateWrittenBackup(ctx, cfg, store, backupDB, encryptor, meta)
		now := time.Now()
		meta.ValidatedAt = &now
		if validateErr != nil {
			meta.ValidationResult = types.ValidationFailed
			// Record the failure before surfacing it: a backup that exists but
			// failed its restore test must never look unvalidated to the next
			// run's parent resolution.
			_ = WriteMeta(ctx, store, meta, key)
			return nil, validateErr
		}
		meta.ValidationResult = types.ValidationOK
		if err := WriteMeta(ctx, store, meta, key); err != nil {
			return nil, fmt.Errorf("failed to record validation result: %w", err)
		}
	}

	return meta.ToResult(ObjectLocation(store, art.FileName), ObjectLocation(store, art.ChecksumName)), nil
}

// writeEncryptedArchive is the type-agnostic core: dump into a zip in a
// goroutine, stream it through an io.Pipe into the encryptor, which writes
// into a Storer object, commit it, write the checksum.
//
// Nothing here knows or cares what the driver put in the archive, which is why
// the physical path needs no changes to it. Nothing here knows whether the
// object lands in a directory or a bucket either: the Storer's Create/Commit
// contract is what the old "temp file, fsync, rename" sequence promised.
func writeEncryptedArchive(
	ctx context.Context,
	cfg types.Config,
	store interfaces.Storer,
	backupDB interfaces.BackupDB,
	encryptor interfaces.Encryptor,
	req types.BackupRequest,
) (*archiveArtifact, *types.DumpOutcome, error) {

	startedAt := time.Now()
	// The ID is UTC on purpose. Formatted in local time, the hour that repeats
	// at the end of daylight saving time produced the same ID twice, and the
	// second run renamed its archive over the first and overwrote its sidecar.
	id := startedAt.UTC().Format(backupTimeLayout)
	encName := archiveName(id)
	sumName := checksumName(encName)

	// Belt and braces for the same hazard: whatever produced the collision, a
	// backup must never replace another backup. Commit would overwrite, so
	// the check has to happen here.
	for _, existing := range []string{encName, types.MetaFileName(id)} {
		_, err := store.Stat(ctx, existing)
		if err == nil {
			return nil, nil, fmt.Errorf("refusing to overwrite %s: a backup with ID %s already exists", ObjectLocation(store, existing), id)
		}
		if !errors.Is(err, types.ErrObjectNotExist) {
			return nil, nil, fmt.Errorf("cannot check for an existing %s: %w", existing, err)
		}
	}

	w, err := store.Create(ctx, encName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create archive object: %w", err)
	}
	// Abort after a successful Commit is a no-op, so this covers every early
	// return below without a flag.
	defer w.Abort()
	counted := &countingWriter{w: w}

	pr, pw := io.Pipe()

	var (
		outcome *types.DumpOutcome
		dumpErr error
		done    = make(chan struct{})
	)
	go func() {
		defer close(done)
		zw := zip.NewWriter(pw)
		outcome, dumpErr = backupDB.DumpToArchive(ctx, zw, req)
		closeErr := zw.Close()
		if dumpErr == nil && closeErr != nil {
			dumpErr = closeErr
		}
		if dumpErr != nil {
			pw.CloseWithError(dumpErr)
			return
		}
		pw.Close()
	}()

	hexChecksum, err := encryptor.EncryptStream(pr, counted, cfg.EncryptPassphrase)

	// Unblock the producer before waiting on it. EncryptStream returns on its
	// first failed write — a full backup mount or a failed upload being the
	// likeliest causes — without draining the pipe. That leaves the dump
	// goroutine blocked in pw.Write forever, so the <-done below would never
	// return and the process would hang holding the dump open instead of
	// reporting the error.
	if err != nil {
		pr.CloseWithError(err)
	}

	// Wait for the producer before reading outcome/dumpErr — without this the
	// read races the goroutine's write.
	<-done

	if err != nil {
		return nil, nil, fmt.Errorf("streaming encryption failed: %w", err)
	}
	if dumpErr != nil {
		return nil, nil, dumpErr
	}
	if outcome == nil {
		return nil, nil, fmt.Errorf("driver %s returned no dump outcome", backupDB.Name())
	}

	// Durability lives in Commit: for the file backend that is fsync, rename,
	// fsync dir; for S3 it is the completed upload being acknowledged. Either
	// way, when it returns nil the archive is in place under its final name.
	//
	// WriteMeta used to fsync the sidecar while the archive it describes was
	// never flushed, which made the guarantee backwards — a power loss could
	// leave a durable sidecar asserting a size and SHA-256 for an archive that
	// did not survive. A backup that looks valid and is not is worse than one
	// that is obviously missing.
	if err := w.Commit(); err != nil {
		return nil, nil, fmt.Errorf("failed to finalize backup object: %w", err)
	}
	// If the checksum cannot be written we remove the archive too: an archive
	// with no checksum is picked up by findLatestArchive as the newest backup
	// and then cannot be verified, which is worse than no archive at all.
	if err := interfaces.PutObject(ctx, store, sumName, []byte(hexChecksum)); err != nil {
		if derr := store.Delete(ctx, encName); derr != nil {
			log.Printf("WARNING: could not remove %s after its checksum failed to write: %v", encName, derr)
		}
		return nil, nil, fmt.Errorf("failed to write checksum: %w", err)
	}

	return &archiveArtifact{
		ID:           id,
		FileName:     encName,
		ChecksumName: sumName,
		ChecksumHex:  hexChecksum,
		SizeBytes:    counted.n,
		StartedAt:    startedAt,
	}, outcome, nil
}

func buildMeta(dbType, encName string, req types.BackupRequest, art *archiveArtifact, outcome *types.DumpOutcome) types.BackupMeta {
	chainID := art.ID
	parentID := ""
	if req.Parent != nil {
		chainID = req.Parent.ChainID
		parentID = req.Parent.ID
	}
	return types.BackupMeta{
		SchemaVersion:  types.MetaSchemaVersion,
		ID:             art.ID,
		DBType:         dbType,
		Type:           req.Type,
		ChainID:        chainID,
		ParentID:       parentID,
		CreatedAt:      art.StartedAt,
		EncryptedFile:  art.FileName,
		SizeBytes:      art.SizeBytes,
		ChecksumSHA256: art.ChecksumHex,
		EntryCount:     len(outcome.Entries),
		Encryption:     types.EncryptionInfo{Container: encName, KDF: "scrypt"},
		PositionStart:  outcome.PositionStart,
		PositionEnd:    outcome.PositionEnd,
		Timeline:       outcome.Timeline,
	}
}

// validateWrittenBackup assembles the chain ending at meta and hands it to the
// driver. For a full backup that is one decrypt; for an incremental it walks
// ancestors via their sidecars, oldest first.
func validateWrittenBackup(
	ctx context.Context,
	cfg types.Config,
	store interfaces.Storer,
	backupDB interfaces.BackupDB,
	encryptor interfaces.Encryptor,
	meta types.BackupMeta,
) error {
	workdir, err := os.MkdirTemp(cfg.ScratchDir, scratchPrefix+"validation_")
	if err != nil {
		return fmt.Errorf("failed to create validation workdir: %w", err)
	}
	defer os.RemoveAll(workdir)

	chain, err := ExtractChain(ctx, cfg, store, encryptor, meta, workdir)
	if err != nil {
		return err
	}
	if err := backupDB.ValidateChain(ctx, chain); err != nil {
		return fmt.Errorf("database restore test failed: %w", err)
	}
	return nil
}

// ExtractChain decrypts and unzips every member of the chain ending at leaf,
// oldest first, into workdir.
func ExtractChain(
	ctx context.Context,
	cfg types.Config,
	store interfaces.Storer,
	encryptor interfaces.Encryptor,
	leaf types.BackupMeta,
	workdir string,
) ([]types.ExtractedBackup, error) {

	lineage, err := resolveLineage(ctx, store, metaPolicyFor(cfg), leaf)
	if err != nil {
		return nil, err
	}

	out := make([]types.ExtractedBackup, 0, len(lineage))
	for _, m := range lineage {
		memberDir := filepath.Join(workdir, m.ID)
		zipPath := filepath.Join(memberDir, "restore.zip")
		if err := os.MkdirAll(memberDir, 0o700); err != nil {
			return nil, err
		}
		if err := decryptObject(ctx, store, encryptor, m.EncryptedFile, zipPath, cfg.EncryptPassphrase, ""); err != nil {
			return nil, fmt.Errorf("member %s: %w", m.ID, err)
		}
		files, err := Unzip(zipPath, filepath.Join(memberDir, "extract"))
		if err != nil {
			return nil, fmt.Errorf("unzip %s: %w", m.ID, err)
		}
		os.Remove(zipPath) // free scratch as we go; chains get large
		out = append(out, types.ExtractedBackup{Meta: m, Files: files})
	}
	return out, nil
}

// resolveLineage reads the store's sidecars and walks ParentID links from
// leaf back to the chain root, returning the members oldest-first.
func resolveLineage(ctx context.Context, store interfaces.Storer, pol MetaPolicy, leaf types.BackupMeta) ([]types.BackupMeta, error) {
	if leaf.IsChainRoot() {
		return []types.BackupMeta{leaf}, nil
	}
	scan, err := ReadAllMetaWith(ctx, store, pol)
	if err != nil {
		return nil, err
	}
	lineage, err := scan.Lineage(leaf)
	if err != nil {
		return nil, fmt.Errorf("%w (in %s)", err, store.Location())
	}
	return lineage, nil
}
