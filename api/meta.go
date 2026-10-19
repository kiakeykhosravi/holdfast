package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"

	"holdfast/interfaces"
	"holdfast/types"
)

// WriteMeta stores a sidecar. The Storer's Create/Commit contract gives the
// atomicity this used to get from "temp file, fsync, rename": a half-written
// sidecar is worse than a missing one, because retention would read a
// truncated chain and could delete an ancestor whose descendants it failed to
// see.
//
// With a non-nil key the sidecar is signed (see meta_sign.go). With nil it is
// written unsigned, which is what tests and a passphrase-less retention run
// use.
func WriteMeta(ctx context.Context, store interfaces.Storer, meta types.BackupMeta, key []byte) error {
	if meta.SchemaVersion == 0 {
		meta.SchemaVersion = types.MetaSchemaVersion
	}
	if err := meta.Validate(); err != nil {
		return fmt.Errorf("refusing to write invalid metadata for %s: %w", meta.ID, err)
	}
	if key != nil {
		meta.Signature = signMeta(key, meta)
	} else {
		meta.Signature = ""
	}

	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata for %s: %w", meta.ID, err)
	}
	b = append(b, '\n')

	if err := interfaces.PutObject(ctx, store, types.MetaFileName(meta.ID), b); err != nil {
		return fmt.Errorf("write metadata for %s: %w", meta.ID, err)
	}
	return nil
}

// ErrMetaUnsigned is returned wrapped by ReadMeta when a key was supplied but
// the sidecar carries no signature. It is informational: the sidecar is still
// returned, and callers that care (ReadAllMeta) count these for a warning.
var ErrMetaUnsigned = fmt.Errorf("sidecar is not signed")

// ReadMeta reads and validates a single sidecar by object name. A missing
// sidecar is reported with types.ErrObjectNotExist wrapped.
//
// With a non-nil key, a sidecar whose signature does not verify is an error:
// it was edited after being written, or by a different passphrase, and either
// way its contents cannot be trusted to describe a chain. An unsigned sidecar
// is returned together with ErrMetaUnsigned so the caller can decide; both
// values are meaningful in that case.
func ReadMeta(ctx context.Context, store interfaces.Storer, name string, key []byte) (types.BackupMeta, error) {
	var meta types.BackupMeta
	b, err := interfaces.GetObject(ctx, store, name)
	if err != nil {
		return meta, err
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		return meta, fmt.Errorf("parse %s: %w", name, err)
	}
	if err := meta.Validate(); err != nil {
		return meta, fmt.Errorf("invalid %s: %w", name, err)
	}
	if key != nil {
		if meta.Signature == "" {
			return meta, fmt.Errorf("%s: %w", name, ErrMetaUnsigned)
		}
		if !verifyMeta(key, meta) {
			return meta, fmt.Errorf("%s: signature does not verify (edited after writing, or written with a different passphrase)", name)
		}
	}
	return meta, nil
}

// SkippedMeta records a sidecar that could not be read or did not validate.
type SkippedMeta struct {
	Name string
	Err  error
}

// MetaScan is the result of reading every sidecar in the store.
type MetaScan struct {
	// Backups are the sidecars that parsed and validated, oldest first.
	Backups []types.BackupMeta

	// Skipped are the ones that did not. Callers must not treat this as
	// noise: a sidecar that will not parse describes a backup whose
	// dependencies are unknown, and anything that deletes files has to refuse
	// to run rather than assume the file is unreferenced.
	Skipped []SkippedMeta

	// Unsigned counts sidecars that were accepted without a signature check,
	// either because they predate signing or because no key was available.
	Unsigned int
}

// HasSkipped reports whether any sidecar failed to load.
func (s MetaScan) HasSkipped() bool { return len(s.Skipped) > 0 }

// ByID indexes the loaded sidecars.
func (s MetaScan) ByID() map[string]types.BackupMeta {
	out := make(map[string]types.BackupMeta, len(s.Backups))
	for _, m := range s.Backups {
		out[m.ID] = m
	}
	return out
}

// Chains groups the loaded sidecars by chain root. Each slice keeps the
// oldest-first ordering of Backups, which is restore order.
//
// This is grouping only — no expiry policy, no opinion about what is
// deletable. That lives in retention.
func (s MetaScan) Chains() map[string][]types.BackupMeta {
	out := make(map[string][]types.BackupMeta)
	for _, m := range s.Backups {
		out[m.ChainID] = append(out[m.ChainID], m)
	}
	return out
}

// Lineage walks ParentID links from leaf back to its chain root and returns
// the members oldest-first, which is restore order. It fails on a missing
// ancestor or a cycle rather than returning a truncated chain.
func (s MetaScan) Lineage(leaf types.BackupMeta) ([]types.BackupMeta, error) {
	if leaf.IsChainRoot() {
		return []types.BackupMeta{leaf}, nil
	}
	byID := s.ByID()
	lineage := []types.BackupMeta{leaf}
	seen := map[string]bool{leaf.ID: true}
	current := leaf
	for !current.IsChainRoot() {
		parent, ok := byID[current.ParentID]
		if !ok {
			return nil, fmt.Errorf("backup %s references parent %s, which has no readable sidecar",
				current.ID, current.ParentID)
		}
		if seen[parent.ID] {
			return nil, fmt.Errorf("cycle in backup chain at %s", parent.ID)
		}
		seen[parent.ID] = true
		lineage = append(lineage, parent)
		current = parent
	}
	for i, j := 0, len(lineage)-1; i < j; i, j = i+1, j-1 {
		lineage[i], lineage[j] = lineage[j], lineage[i]
	}
	return lineage, nil
}

// MetaPolicy says how far a sidecar is trusted when it is read.
type MetaPolicy struct {
	// Key verifies signatures. nil disables verification entirely, which is
	// what tests and a passphrase-less host get.
	Key []byte

	// RequireSigned treats an unsigned sidecar as untrusted (Skipped) instead
	// of accepting it as one written before signing existed. Without it the
	// signature is only a tamper seal against accidents: anyone who can write
	// the file can also delete the signature field and be believed. It needs
	// a Key; with none there is nothing to verify against, and a scan refuses
	// rather than quietly trusting everything.
	RequireSigned bool
}

// ErrNoMetaKey is returned when RequireSigned is set and no key is available.
var ErrNoMetaKey = errors.New("require_signed_sidecars is set but no passphrase is available to verify them")

// ReadAllMeta loads every backup_*.meta.json in the store, verifying with key
// when it is non-nil and accepting unsigned sidecars. See ReadAllMetaWith.
func ReadAllMeta(ctx context.Context, store interfaces.Storer, key []byte) (MetaScan, error) {
	return ReadAllMetaWith(ctx, store, MetaPolicy{Key: key})
}

// ReadAllMetaWith loads every backup_*.meta.json in the store under pol.
//
// A sidecar that fails to parse or validate is recorded in Skipped rather than
// failing the scan, so one corrupt sidecar cannot block a backup run. It is the
// caller's decision what to do about it — a backup can proceed, a deletion pass
// should not.
//
// A sidecar with a signature that does not verify is Skipped. One with no
// signature at all is accepted and counted in Unsigned unless pol.RequireSigned
// is set, because every sidecar written before signing existed looks like that
// and refusing them by default would brick retention on upgrade. StartBackup
// signs them, after which require_signed_sidecars can be turned on.
func ReadAllMetaWith(ctx context.Context, store interfaces.Storer, pol MetaPolicy) (MetaScan, error) {
	var scan MetaScan
	if pol.RequireSigned && pol.Key == nil {
		return scan, ErrNoMetaKey
	}

	objs, err := store.List(ctx, objectPrefix)
	if err != nil {
		return scan, err
	}

	for _, o := range objs {
		if !isMetaName(o.Name) {
			continue
		}
		meta, err := ReadMeta(ctx, store, o.Name, pol.Key)
		if err != nil {
			if !errors.Is(err, ErrMetaUnsigned) {
				scan.Skipped = append(scan.Skipped, SkippedMeta{Name: o.Name, Err: err})
				continue
			}
			if pol.RequireSigned {
				scan.Skipped = append(scan.Skipped, SkippedMeta{Name: o.Name, Err: fmt.Errorf("%w (require_signed_sidecars is on)", err)})
				continue
			}
			scan.Unsigned++
		}
		scan.Backups = append(scan.Backups, meta)
	}
	if pol.Key == nil {
		scan.Unsigned = len(scan.Backups)
	}
	if scan.Unsigned > 0 && len(scan.Backups) > 0 {
		if pol.Key == nil {
			log.Printf("[meta] WARNING: %d sidecar(s) read without signature verification (no passphrase available)", scan.Unsigned)
		} else {
			log.Printf("[meta] %d of %d sidecar(s) are unsigned; the next backup run signs them, and require_signed_sidecars: true then makes an unsigned sidecar untrusted", scan.Unsigned, len(scan.Backups))
		}
	}

	sort.Slice(scan.Backups, func(i, j int) bool {
		if scan.Backups[i].CreatedAt.Equal(scan.Backups[j].CreatedAt) {
			return scan.Backups[i].ID < scan.Backups[j].ID
		}
		return scan.Backups[i].CreatedAt.Before(scan.Backups[j].CreatedAt)
	})

	return scan, nil
}
