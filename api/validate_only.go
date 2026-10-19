package api

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"holdfast/interfaces"
	"holdfast/types"
)

// ValidateEncryptedBackup decrypts an existing backup, validates the chain it
// belongs to, and records the verdict in the backup's sidecar.
//
// name is the archive's object name in the store. If empty, the newest
// backup_*.zip.enc is used.
//
// # Why it writes the sidecar
//
// Parent resolution refuses to extend a chain whose tip has not passed a
// restore test. Without recording the result here there was no way to prove a
// backup restorable out of band, so one failed or skipped validation blocked
// every subsequent incremental with no operator recourse except taking a full.
//
// # Why it takes the run lock
//
// It writes the same sidecar a concurrent backup run may be writing, and it
// stages in the same scratch_dir that SweepStaleTemps cleans. Unlike a
// scheduled backup, contention here is an error rather than a skip: this is
// an operator asking a question, and "try again later" is the answer they
// need to hear.
func ValidateEncryptedBackup(ctx context.Context, cfg types.Config, store interfaces.Storer, backupDB interfaces.BackupDB, encryptor interfaces.Encryptor, passphrase string, name string) (*types.ValidateOnlyResult, error) {
	unlock, err := AcquireRunLock(LockDir(cfg))
	if err != nil {
		return nil, fmt.Errorf("cannot validate while %w", err)
	}
	defer unlock()
	SweepStaleTemps(ctx, cfg, store)

	key := MetaKey(passphrase)

	name = strings.TrimSpace(name)
	if name == "" {
		var err error
		name, err = findLatestArchive(ctx, store)
		if err != nil {
			return nil, err
		}
	}
	if !isArchiveName(name) {
		return nil, fmt.Errorf("%q is not a backup archive name (want backup_<id>.zip.enc)", name)
	}

	sumName := checksumName(name)
	checksumOK := (*bool)(nil)
	if hexSum, ok, err := readChecksumHex(ctx, store, sumName); err != nil {
		return nil, err
	} else if ok {
		ok2, err := verifyObjectSHA256(ctx, store, name, hexSum)
		if err != nil {
			return nil, err
		}
		checksumOK = &ok2
		if !ok2 {
			return nil, fmt.Errorf("checksum mismatch for %s", ObjectLocation(store, name))
		}
	}

	// Stage in scratch_dir, never /tmp: validating a physical chain extracts
	// several full copies of the cluster, and /tmp is tmpfs on most hosts.
	workdir, err := os.MkdirTemp(cfg.ScratchDir, scratchPrefix+"testonly_")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(workdir)

	meta, synthesized, err := metaForValidation(ctx, store, name, key, cfg.RequireSignedSidecars)
	if err != nil {
		return nil, err
	}
	chain, err := ExtractChain(ctx, cfg, store, encryptor, meta, workdir)
	if err != nil {
		return nil, err
	}

	// A synthesized sidecar guessed the medium. Correct it from what the
	// archive actually holds before dispatching, so a physical archive whose
	// sidecar was lost is not pushed down the logical path to fail obscurely.
	if synthesized && len(chain) > 0 {
		if detected := detectMedium(chain[0].Files); detected != "" && detected != chain[0].Meta.Type.Medium {
			log.Printf("[validate] %s has no sidecar; archive contents indicate %s, not %s",
				meta.ID, detected, chain[0].Meta.Type.Medium)
			chain[0].Meta.Type.Medium = detected
			if detected == types.MediumPhysical {
				log.Printf("[validate] WARNING: without a sidecar the chain cannot be walked, so %s is validated standalone; an incremental will fail here because its ancestors are unknown", meta.ID)
			}
		}
	}

	validateErr := backupDB.ValidateChain(ctx, chain)

	// Record the verdict on the real sidecar. A synthesized one is not written
	// back: inventing a sidecar would assert a type and chain we only guessed.
	if !synthesized {
		now := time.Now()
		meta.ValidatedAt = &now
		if validateErr != nil {
			meta.ValidationResult = types.ValidationFailed
		} else {
			meta.ValidationResult = types.ValidationOK
		}
		if werr := WriteMeta(ctx, store, meta, key); werr != nil {
			log.Printf("WARNING: could not record validation result for %s: %v", meta.ID, werr)
		}
	}

	if validateErr != nil {
		return nil, validateErr
	}

	return &types.ValidateOnlyResult{
		Mode:         string(types.RunModeValidateOnly),
		Input:        ObjectLocation(store, name),
		ChecksumFile: ObjectLocation(store, sumName),
		ChecksumOK:   checksumOK,
		DockerImage:  cfg.PGTestDockerImage,
		ChainMembers: len(chain),
		MetaRecorded: !synthesized,
		TestedOK:     true,
		CreatedAt:    time.Now(),
	}, nil
}

// detectMedium infers the backup medium from the archive's entries.
func detectMedium(files []string) types.Medium {
	for _, f := range files {
		switch filepath.Base(f) {
		case "base.tar", StateEntryName:
			return types.MediumPhysical
		case "globals.sql":
			return types.MediumLogical
		}
	}
	return ""
}

// metaForValidation loads the sidecar for an archive so the chain can be walked.
//
// synthesized reports that no sidecar existed and the returned value is a
// guess. Backups taken before sidecars existed have none; so does a run that
// died between the archive commit and WriteMeta. The guess is a self-contained
// logical full, because that is what every pre-sidecar backup was — but the
// caller must correct it from the archive's contents rather than trust it,
// since a physical archive can also reach here.
//
// An unsigned sidecar is accepted unless requireSigned is set: it predates
// signing, and the point of this mode is to prove such a backup restorable. A
// sidecar whose signature does not verify is refused; recording a verdict
// against a chain description that has been tampered with would launder it.
func metaForValidation(ctx context.Context, store interfaces.Storer, name string, key []byte, requireSigned bool) (types.BackupMeta, bool, error) {
	id := idFromArchiveName(name)

	meta, err := ReadMeta(ctx, store, types.MetaFileName(id), key)
	if err == nil || (errors.Is(err, ErrMetaUnsigned) && !requireSigned) {
		return meta, false, nil
	}
	if errors.Is(err, ErrMetaUnsigned) {
		return types.BackupMeta{}, false, fmt.Errorf("%w; a backup run signs legacy sidecars, or set require_signed_sidecars: false for this host", err)
	}
	if !errors.Is(err, types.ErrObjectNotExist) {
		return types.BackupMeta{}, false, err
	}

	info, statErr := store.Stat(ctx, name)
	if statErr != nil {
		return types.BackupMeta{}, false, statErr
	}
	return types.BackupMeta{
		SchemaVersion: types.MetaSchemaVersion,
		ID:            id,
		Type:          types.LogicalFull,
		ChainID:       id,
		CreatedAt:     info.ModTime,
		EncryptedFile: name,
	}, true, nil
}
