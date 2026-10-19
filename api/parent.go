package api

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	"holdfast/interfaces"
	"holdfast/types"
)

// StatePath is where the parent-state cache keeps a backup's driver artifact —
// for Postgres, its backup_manifest.
//
// The cache is disposable: losing it costs a decrypt of the parent archive, not
// correctness.
func StatePath(cfg types.Config, id string) string {
	return filepath.Join(cfg.StateDir, "state", id)
}

// SelectParentMeta picks the sidecar an incremental should build on, without
// touching the archive itself.
//
// This is split from state materialisation so -dry-run can report the parent
// it *would* use without the side effects: EnsureState decrypts a whole
// archive and writes to state_dir, which has no business happening during a
// dry run. It also lets StartBackup run the preflight gate before paying for
// that decrypt, so a node that is about to be told "wrong role, skip" does not
// first decrypt a cluster-sized archive for nothing.
//
// ok is false when the requested type needs no parent.
//
// Every "an incremental is not possible right now" condition is reported as
// *types.IncrementalUnavailableError, which is what lets StartBackup downgrade
// to a full. Conditions that are operator error — an explicit -parent that does
// not exist or is the wrong medium — stay plain errors, because silently taking
// a different backup than the one asked for would hide a mistake.
func SelectParentMeta(ctx context.Context, cfg types.Config, store interfaces.Storer, requested types.BackupType, explicitID string) (types.BackupMeta, bool, error) {
	var zero types.BackupMeta

	if !requested.RequiresParent() {
		if explicitID != "" {
			return zero, false, fmt.Errorf("-parent is meaningless for %s", requested)
		}
		return zero, false, nil
	}

	scan, err := ReadAllMetaWith(ctx, store, metaPolicyFor(cfg))
	if err != nil {
		return zero, false, fmt.Errorf("cannot read backup metadata from %s: %w", store.Location(), err)
	}

	if explicitID != "" {
		meta, found := scan.ByID()[explicitID]
		if !found {
			return zero, false, fmt.Errorf("no backup with ID %q in %s", explicitID, store.Location())
		}
		if meta.Type.Medium != requested.Medium {
			// Caught here rather than inside EnsureState, which would decrypt
			// the entire archive before discovering it holds no driver state.
			return zero, false, fmt.Errorf("-parent %s is a %s backup, which cannot be the parent of %s",
				explicitID, meta.Type, requested)
		}
		if err := lineageInStore(ctx, scan, store, meta); err != nil {
			return zero, false, fmt.Errorf("-parent %s: %w", explicitID, err)
		}
		return meta, true, nil
	}

	meta, err := newestUsableChainMember(scan, cfg, requested)
	if err != nil {
		return zero, false, err
	}
	if err := lineageInStore(ctx, scan, store, meta); err != nil {
		// A chain whose archives are not all present cannot be extended, and
		// nothing a retry would change. Take a full.
		return zero, false, &types.IncrementalUnavailableError{
			Reason: fmt.Sprintf("newest %s backup %s cannot be extended: %v", requested.Medium, meta.ID, err),
		}
	}
	return meta, true, nil
}

// lineageInStore checks that every member of the chain ending at tip has a
// readable sidecar and an archive that still exists.
//
// # Why this exists
//
// Parent selection used to trust the sidecar alone. With a warm state cache
// nothing ever opened the parent archive, so an incremental could be taken
// against a base that had been deleted by hand or lost with a disk, and with
// restore tests off the resulting chain was silently unrestorable. With a cold
// cache the decrypt failed with a plain error instead, which exited 1 on every
// run until someone intervened — the exact failure mode fallback_to_full was
// meant to remove.
func lineageInStore(ctx context.Context, scan MetaScan, store interfaces.Storer, tip types.BackupMeta) error {
	lineage, err := scan.Lineage(tip)
	if err != nil {
		return err
	}
	for _, m := range lineage {
		if _, err := store.Stat(ctx, m.EncryptedFile); err != nil {
			return fmt.Errorf("archive %s for chain member %s is not in %s: %w", m.EncryptedFile, m.ID, store.Location(), err)
		}
	}
	return nil
}

// newestUsableChainMember picks the tip of the newest chain that is still
// eligible to be extended.
//
// Every rejection here is an IncrementalUnavailableError, never a hard failure.
// That distinction is the whole point: these conditions all arise on a healthy
// system during normal operation — a chain reaching its length limit, a fresh
// host with no backups yet, a tip whose restore test failed — and the correct
// response to each is to take a full backup, not to abort the run. Returning
// plain errors here is what used to brick a scheduled incremental permanently,
// because main exited 2 and no backup was taken at all until a human
// intervened. The config comment promising "force a new full once a chain
// reaches this many members" is only true with this in place.
func newestUsableChainMember(scan MetaScan, cfg types.Config, requested types.BackupType) (types.BackupMeta, error) {
	var zero types.BackupMeta

	if len(scan.Backups) == 0 {
		return zero, &types.IncrementalUnavailableError{
			Reason: "no existing backups to build an incremental on",
		}
	}

	// Only chains of the same medium are candidates. A store commonly holds
	// two unrelated schedules — a logical job with per-database excludes, and a
	// physical job for whole-cluster incrementals — and the newest backup
	// overall is frequently the wrong one to extend. Rooting a physical
	// incremental on a logical dump fails deep inside pg_basebackup with a
	// confusing message about a missing manifest.
	var tip types.BackupMeta
	found := false
	for i := len(scan.Backups) - 1; i >= 0; i-- {
		if scan.Backups[i].Type.Medium == requested.Medium {
			tip = scan.Backups[i]
			found = true
			break
		}
	}
	if !found {
		return zero, &types.IncrementalUnavailableError{
			Reason: fmt.Sprintf("no %s backup to build %s on", requested.Medium, requested),
		}
	}

	// Chain length is both restore time and blast radius: one corrupt full
	// destroys every incremental behind it.
	members := scan.Chains()[tip.ChainID]
	if maxLen := cfg.MaxChainLength; maxLen > 0 && len(members) >= maxLen {
		return zero, &types.IncrementalUnavailableError{
			Reason: fmt.Sprintf("chain %s already has %d member(s) (max_chain_length=%d)", tip.ChainID, len(members), maxLen),
		}
	}

	// Chaining onto a backup that has never been proven restorable compounds a
	// problem instead of catching it.
	//
	// Only meaningful when restore tests are enabled: with
	// enable_restore_test: false nothing ever sets validationResult, so every
	// tip looks unproven and demanding proof would make incrementals outright
	// impossible rather than merely unverified.
	if cfg.EnableRestoreTest && !tip.ValidatedOK() {
		return zero, &types.IncrementalUnavailableError{
			Reason: fmt.Sprintf("newest %s backup %s has not passed a restore test (validationResult=%q)",
				requested.Medium, tip.ID, tip.ValidationResult),
		}
	}
	return tip, nil
}

// StateEntryName is the archive entry holding a backup's driver state. Postgres
// names it backup_manifest; a second engine would need this to come from the
// driver rather than a constant.
const StateEntryName = "backup_manifest"

// ParentRefFor builds the reference a request carries for its parent. It does
// not touch the archive: StatePath names where the parent's driver state will
// be once MaterialiseParentState has run, and may not exist yet.
func ParentRefFor(cfg types.Config, meta types.BackupMeta) *types.ParentRef {
	return &types.ParentRef{
		ID:        meta.ID,
		Type:      meta.Type,
		ChainID:   meta.ChainID,
		StatePath: StatePath(cfg, meta.ID),
		Timeline:  meta.Timeline,
	}
}

// MaterialiseParentState makes the parent's driver state available at the
// path ParentRefFor promised, decrypting the parent archive if the cache is
// cold.
//
// Failure is reported as IncrementalUnavailableError. The archive may be
// corrupt, unreadable, or encrypted under a different passphrase; in every
// case an incremental is impossible and a full backup is the right response.
// It is logged loudly because, unlike the other fallback reasons, this one is
// not something a healthy system does.
func MaterialiseParentState(ctx context.Context, cfg types.Config, store interfaces.Storer, encryptor interfaces.Encryptor, meta types.BackupMeta) error {
	if _, err := EnsureState(ctx, cfg, store, encryptor, meta, StateEntryName); err != nil {
		log.Printf("WARNING: parent %s cannot be used: %v", meta.ID, err)
		return &types.IncrementalUnavailableError{
			Reason: fmt.Sprintf("parent %s: %v", meta.ID, err),
		}
	}
	return nil
}
