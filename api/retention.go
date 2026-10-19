package api

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"holdfast/interfaces"
	"holdfast/types"
)

// backupTimeLayout matches the object name format: backup_2006-01-02_15-04-05.zip.enc
// The timestamp is UTC (see writeEncryptedArchive).
const backupTimeLayout = "2006-01-02_15-04-05"

// parseBackupTime extracts the timestamp from a backup object name.
// Expected format: backup_<YYYY-MM-DD_HH-MM-SS>.zip.enc (or .zip.enc.sha256)
// Returns zero time if the name doesn't match.
//
// Parsed as UTC because IDs are now generated in UTC. Archives named before
// that change were local time; for them the result is off by the zone offset,
// which only matters for sidecar-less archives and is a few hours against a
// cutoff measured in days.
func parseBackupTime(base string) time.Time {
	s := base
	s = strings.TrimSuffix(s, checksumSuffix)
	s = strings.TrimSuffix(s, archiveSuffix)
	s = strings.TrimPrefix(s, objectPrefix)
	t, err := time.ParseInLocation(backupTimeLayout, s, time.UTC)
	if err != nil {
		return time.Time{}
	}
	return t
}

// RetentionStats counts what a cleanup pass did.
type RetentionStats struct {
	ChainsDeleted  int `json:"chainsDeleted"`
	BackupsDeleted int `json:"backupsDeleted"`
	FilesDeleted   int `json:"filesDeleted"`
	ChainsKept     int `json:"chainsKept"`
	// ChainsKeptByFloor is how many of ChainsKept had aged out and survive
	// only because of MinChainsKept.
	ChainsKeptByFloor int `json:"chainsKeptByFloor"`
	OrphansDeleted    int `json:"orphansDeleted"`
}

// RetentionPolicy is what a cleanup pass needs to decide, separated from the
// config so the decision can be exercised without one.
type RetentionPolicy struct {
	// Days is the age past which a chain may go. 0 disables retention.
	Days int
	// MinChainsKept is the number of newest chains that are never deleted,
	// however old. 0 disables the floor.
	MinChainsKept int
	// Meta says how sidecars are trusted. Retention refuses to delete while
	// any sidecar is untrusted under it.
	Meta MetaPolicy
}

func retentionPolicyFor(cfg types.Config) RetentionPolicy {
	return RetentionPolicy{Days: cfg.RetentionDays, MinChainsKept: cfg.MinChainsKept, Meta: metaPolicyFor(cfg)}
}

// RunRetention takes the run lock and runs a cleanup pass.
//
// Retention used to run unlocked, after StartBackup had released the lock.
// Another run's parent resolution could then read a chain while this pass was
// deleting it. Contention is reported as a SkipError: the other run will do
// its own retention when it finishes.
func RunRetention(ctx context.Context, cfg types.Config, store interfaces.Storer) (RetentionStats, error) {
	unlock, err := AcquireRunLock(LockDir(cfg))
	if err != nil {
		var held *LockHeldError
		if errors.As(err, &held) {
			return RetentionStats{}, &SkipError{Reason: "retention: " + held.Error()}
		}
		return RetentionStats{}, fmt.Errorf("retention: cannot take the run lock: %w", err)
	}
	defer unlock()
	return CleanupRetentionStats(ctx, store, retentionPolicyFor(cfg))
}

// CleanupRetentionStats deletes backup sets whose age exceeds pol.Days and
// reports what it did. Callers that need the run lock go through RunRetention.
//
// # Chain safety
//
// This is the property the previous filename-only implementation lacked, and
// the reason it could destroy data. Deleting by timestamp alone removes a
// physical full as soon as it ages out, orphaning every incremental built on
// it — the incrementals survive, are younger than the cutoff, and are
// individually worthless because pg_combinebackup cannot run without the base.
//
// So: a chain is deleted whole or not at all, and only once its *newest*
// member has aged out. That keeps a chain alive slightly longer than a pure
// age policy would, which is the correct direction to err for a backup tool.
//
// The archive, its .sha256 and its .meta.json are deleted together. Leaving
// the sidecar behind was its own bug: ReadAllMeta accepts it as a valid
// backup, so parent resolution can select a tip whose archive no longer
// exists, and chain validation can try to walk into a deleted ancestor.
//
// # The floor
//
// Age alone has a failure mode this tool used to share with every naive
// retention script: retention runs after every backup attempt whether or not
// the backup succeeded, so a host whose dumps fail for more nights than
// retention_days ends up with no backups at all, every run exiting 1 and the
// last good chain gone. MinChainsKept is the stop on that: the newest N chains
// are kept whatever their age. The default is 1 — never delete the last
// chain — and 0 is an explicit opt-out.
//
// # What this is not
//
// Not a GFS policy engine. There is no grandfather/father/son tiering, no
// "keep N monthlies". Architecture.md §7 describes the pure
// Plan(backups, policy, now) / Apply(plan) split that should replace this;
// what follows is the safety floor, not that design.
func CleanupRetentionStats(ctx context.Context, store interfaces.Storer, pol RetentionPolicy) (RetentionStats, error) {
	var stats RetentionStats
	if pol.Days <= 0 {
		return stats, nil
	}
	cutoff := time.Now().Add(-time.Duration(pol.Days) * 24 * time.Hour)

	scan, err := ReadAllMetaWith(ctx, store, pol.Meta)
	if err != nil {
		return stats, fmt.Errorf("retention: cannot read backup metadata: %w", err)
	}

	// A sidecar that will not parse, or whose signature does not verify,
	// describes a backup whose dependents are unknown. Anything that deletes
	// has to refuse rather than assume a file is unreferenced — this is the
	// MetaScan.Skipped contract.
	if scan.HasSkipped() {
		var names []string
		for _, s := range scan.Skipped {
			names = append(names, fmt.Sprintf("%s (%v)", s.Name, s.Err))
		}
		return stats, fmt.Errorf("retention: refusing to delete anything while %d sidecar(s) cannot be trusted, because their dependents are unknown: %s",
			len(scan.Skipped), strings.Join(names, "; "))
	}

	// Archives covered by a sidecar are governed by chain rules below; every
	// other backup_*.zip.enc is handled by the name fallback afterwards.
	described := make(map[string]bool, len(scan.Backups))
	for _, m := range scan.Backups {
		described[m.EncryptedFile] = true
	}

	// Chains newest-first by their newest member, so the floor can be read
	// off the position: the first MinChainsKept chains are untouchable.
	chains := scan.Chains()
	order := make([]string, 0, len(chains))
	for id := range chains {
		order = append(order, id)
	}
	sort.Slice(order, func(i, j int) bool {
		a, b := newestMember(chains[order[i]]), newestMember(chains[order[j]])
		if a.CreatedAt.Equal(b.CreatedAt) {
			return order[i] > order[j]
		}
		return a.CreatedAt.After(b.CreatedAt)
	})

	for rank, chainID := range order {
		members := chains[chainID]
		newest := newestMember(members)
		if !newest.CreatedAt.Before(cutoff) {
			stats.ChainsKept++
			continue
		}
		if rank < pol.MinChainsKept {
			log.Printf("[retention] keeping chain %s although its newest member (%s) has aged out: it is among the %d newest chain(s) (min_chains_kept)",
				chainID, newest.CreatedAt.Format(time.RFC3339), pol.MinChainsKept)
			stats.ChainsKept++
			stats.ChainsKeptByFloor++
			continue
		}

		log.Printf("[retention] deleting chain %s (%d member(s), newest %s)",
			chainID, len(members), newest.CreatedAt.Format(time.RFC3339))
		for _, m := range members {
			n := removeBackupSet(ctx, store, m)
			stats.FilesDeleted += n
			stats.BackupsDeleted++
		}
		stats.ChainsDeleted++
	}
	if len(order) > 0 && stats.ChainsDeleted == len(order) {
		log.Printf("[retention] WARNING: every chain in %s had aged out and min_chains_kept is 0, so no described backup remains", store.Location())
	}

	// Archives with no sidecar: pre-sidecar backups, or a run that died between
	// the commit and WriteMeta. An archive without a sidecar can never be
	// selected as a parent (parent resolution reads sidecars only), so nothing
	// can depend on it and age alone is a safe criterion.
	objs, err := store.List(ctx, objectPrefix)
	if err != nil {
		return stats, err
	}
	for _, o := range objs {
		base := o.Name
		isEnc := strings.HasSuffix(base, archiveSuffix)
		isSha := strings.HasSuffix(base, archiveSuffix+checksumSuffix)
		if !isEnc && !isSha {
			continue
		}
		if isEnc && described[base] {
			continue // governed by the chain rules above
		}
		if isSha && described[strings.TrimSuffix(base, checksumSuffix)] {
			continue
		}

		backupTime := parseBackupTime(base)
		if backupTime.IsZero() {
			// Name doesn't match our format; skip rather than accidentally deleting.
			log.Printf("[retention] skipping unrecognised backup name: %s", base)
			continue
		}
		if !backupTime.Before(cutoff) {
			continue
		}

		if isEnc {
			log.Printf("[retention] deleting un-described backup: %s", base)
			if store.Delete(ctx, base) == nil {
				stats.FilesDeleted++
				stats.OrphansDeleted++
			}
			if store.Delete(ctx, checksumName(base)) == nil {
				stats.FilesDeleted++
			}
			continue
		}
		// An aged checksum whose archive is gone or un-described.
		log.Printf("[retention] deleting orphan checksum: %s", base)
		if store.Delete(ctx, base) == nil {
			stats.FilesDeleted++
			stats.OrphansDeleted++
		}
	}

	return stats, nil
}

// newestMember is the last of an oldest-first chain.
func newestMember(members []types.BackupMeta) types.BackupMeta { return members[len(members)-1] }

// removeBackupSet deletes one backup's archive, checksum and sidecar, and
// reports how many objects actually went away.
//
// The sidecar is removed last: if the process dies midway, an archive with a
// surviving sidecar is a detectable inconsistency, whereas a sidecar pointing
// at a deleted archive is the exact state that breaks parent resolution.
func removeBackupSet(ctx context.Context, store interfaces.Storer, m types.BackupMeta) int {
	n := 0
	if err := store.Delete(ctx, m.EncryptedFile); err == nil {
		n++
	} else if !errors.Is(err, types.ErrObjectNotExist) {
		log.Printf("[retention] WARNING: could not delete %s: %v", m.EncryptedFile, err)
	}
	if err := store.Delete(ctx, checksumName(m.EncryptedFile)); err == nil {
		n++
	}
	if err := store.Delete(ctx, types.MetaFileName(m.ID)); err == nil {
		n++
	} else if !errors.Is(err, types.ErrObjectNotExist) {
		log.Printf("[retention] WARNING: could not delete sidecar for %s: %v", m.ID, err)
	}
	return n
}

func RunRetentionOnly(ctx context.Context, cfg types.Config, store interfaces.Storer) (*types.RetentionResult, error) {
	stats, err := RunRetention(ctx, cfg, store)
	if err != nil {
		return nil, err
	}
	return &types.RetentionResult{
		Mode:              string(types.RunModeRetentionOnly),
		Storage:           store.Location(),
		RetentionDays:     cfg.RetentionDays,
		MinChainsKept:     cfg.MinChainsKept,
		ChainsDeleted:     stats.ChainsDeleted,
		BackupsDeleted:    stats.BackupsDeleted,
		FilesDeleted:      stats.FilesDeleted,
		ChainsKept:        stats.ChainsKept,
		ChainsKeptByFloor: stats.ChainsKeptByFloor,
		OrphansDeleted:    stats.OrphansDeleted,
		CreatedAt:         time.Now(),
	}, nil
}
