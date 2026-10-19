package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"time"

	"holdfast/interfaces"
	"holdfast/types"
)

// smallObjectLimit is the size below which sync compares objects by content
// rather than by size. Sidecars are rewritten in place when a validation
// verdict is recorded, and a rewrite can keep the same length.
const smallObjectLimit = 1 << 20

// SyncOptions controls a sync pass.
type SyncOptions struct {
	// Prune deletes objects from a mirror that the primary no longer has.
	// Off by default: deleting from the offsite copy because the primary
	// looked empty is the one way this mode can destroy data, so it refuses
	// to prune when the primary lists nothing at all, and when any primary
	// sidecar cannot be trusted (the same rule retention follows).
	Prune bool
}

// SyncSource is one backup set to sync: its primary store and the mirrors
// that should match it. main builds one per configured source, each mirror
// already scoped to that source's prefix.
type SyncSource struct {
	Name    string
	Primary interfaces.Storer
	Mirrors []interfaces.Storer
	// Err is set when the source's stores could not be built; the source is
	// reported as failed and the run moves on to the next one.
	Err error
}

// SyncMirrors syncs a single source. It is SyncSources for the common case
// and keeps the pre-sources call shape.
func SyncMirrors(ctx context.Context, cfg types.Config, primary interfaces.Storer, mirrors []interfaces.Storer, opts SyncOptions) (*types.SyncResult, error) {
	return SyncSources(ctx, cfg, []SyncSource{{Primary: primary, Mirrors: mirrors}}, opts)
}

// SyncSources copies everything each source's primary holds to that source's
// mirrors, and optionally removes from the mirrors what the primary has lost.
// Sources run one after another under a single run lock; a source failing
// does not stop the next, and the result carries one entry per source.
//
// # Why this exists
//
// Mirroring at write time (storage.mirrors on the backup host) sends every
// byte through the database host's uplink while the dump is running. When
// that link is slow, or belongs to the database, the copy is better made
// from wherever backup_dir is served: the NFS server, a backup appliance, a
// host with the fat pipe. This mode is that: the database host backs up with
// no mirrors, and a second host runs -sync-mirrors on a schedule.
//
// # What it guarantees
//
// Archives and checksums are copied before sidecars, so a mirror never holds
// a sidecar whose archive is missing — that is the state that breaks chain
// walking on a restore. Each archive is hashed while it streams and compared
// with its .sha256, so a locally corrupted archive is reported rather than
// replicated. Pruning removes sidecars first, for the same reason in reverse.
//
// It takes this host's run lock so two syncs cannot overlap, but nothing
// serialises it against the backup host: a retention pass there can delete
// an archive mid-copy, which surfaces as one failed object and succeeds on the
// next run.
func SyncSources(ctx context.Context, cfg types.Config, sources []SyncSource, opts SyncOptions) (*types.SyncResult, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("nothing to sync: no sources selected")
	}
	for _, src := range sources {
		if src.Err == nil && len(src.Mirrors) == 0 {
			return nil, fmt.Errorf("no mirrors configured: set storage.mirrors (e.g. [s3]) on this host")
		}
	}
	unlock, err := AcquireRunLock(LockDir(cfg))
	if err != nil {
		var held *LockHeldError
		if errors.As(err, &held) {
			return nil, &SkipError{Reason: "sync: " + held.Error()}
		}
		return nil, fmt.Errorf("sync: cannot take the run lock: %w", err)
	}
	defer unlock()

	result := &types.SyncResult{
		Mode:      string(types.RunModeSyncMirrors),
		CreatedAt: time.Now(),
	}
	for _, src := range sources {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if src.Name != "" {
			log.Printf("[sync] source %s", src.Name)
		}
		sr := syncSource(ctx, cfg, src, opts)
		result.Sources = append(result.Sources, sr)
		result.Failed += sr.Failed
	}
	return result, nil
}

// syncSource syncs one source's primary to its mirrors.
func syncSource(ctx context.Context, cfg types.Config, src SyncSource, opts SyncOptions) types.SourceSyncResult {
	sr := types.SourceSyncResult{Name: src.Name}
	if src.Err != nil {
		sr.Error = src.Err.Error()
		sr.Failed = 1
		log.Printf("[sync] FAILED source %s: %v", src.Name, src.Err)
		return sr
	}
	primary := src.Primary
	sr.Primary = primary.Location()

	have, err := primary.List(ctx, objectPrefix)
	if err != nil {
		sr.Error = fmt.Sprintf("list %s: %v", primary.Location(), err)
		sr.Failed = 1
		log.Printf("[sync] FAILED source %s: %s", src.Name, sr.Error)
		return sr
	}
	have = backupObjectsOnly(have)
	sortForCopy(have)

	pruneAllowed := opts.Prune
	if opts.Prune {
		switch {
		case len(have) == 0:
			sr.PruneRefused = fmt.Sprintf("refusing to prune: %s lists no backup objects at all, which is what an unmounted backup_dir looks like", primary.Location())
		default:
			scan, err := ReadAllMeta(ctx, primary, metaKeyFor(cfg))
			if err != nil {
				sr.PruneRefused = fmt.Sprintf("refusing to prune: cannot read primary sidecars: %v", err)
			} else if scan.HasSkipped() {
				sr.PruneRefused = fmt.Sprintf("refusing to prune: %d primary sidecar(s) cannot be trusted, so their dependents are unknown", len(scan.Skipped))
			}
		}
		if sr.PruneRefused != "" {
			pruneAllowed = false
			log.Printf("[sync] WARNING: %s", sr.PruneRefused)
		}
	}

	for _, m := range src.Mirrors {
		mr := syncOne(ctx, primary, m, have, pruneAllowed)
		sr.Mirrors = append(sr.Mirrors, mr)
		sr.Failed += len(mr.Failed)
	}
	return sr
}

func syncOne(ctx context.Context, primary, mirror interfaces.Storer, have []types.ObjectInfo, prune bool) types.MirrorSyncResult {
	mr := types.MirrorSyncResult{Mirror: mirror.Location()}

	there, err := mirror.List(ctx, objectPrefix)
	if err != nil {
		mr.Failed = append(mr.Failed, fmt.Sprintf("list %s: %v", mirror.Location(), err))
		return mr
	}
	onMirror := make(map[string]types.ObjectInfo, len(there))
	for _, o := range there {
		onMirror[o.Name] = o
	}

	for _, o := range have {
		if err := ctx.Err(); err != nil {
			mr.Failed = append(mr.Failed, fmt.Sprintf("%s: %v", o.Name, err))
			return mr
		}
		upToDate, err := sameObject(ctx, primary, mirror, o, onMirror)
		if err != nil {
			mr.Failed = append(mr.Failed, fmt.Sprintf("%s: compare: %v", o.Name, err))
			continue
		}
		if upToDate {
			mr.UpToDate++
			continue
		}
		start := time.Now()
		n, err := copyObject(ctx, primary, mirror, o.Name)
		if err != nil {
			log.Printf("[sync] FAILED %s → %s: %v", o.Name, mirror.Location(), err)
			mr.Failed = append(mr.Failed, fmt.Sprintf("%s: %v", o.Name, err))
			continue
		}
		log.Printf("[sync] copied %s (%s) to %s in %s", o.Name, FormatBytes(n), mirror.Location(), time.Since(start).Round(time.Second))
		mr.Copied++
		mr.CopiedBytes += n
	}

	if !prune {
		return mr
	}
	onPrimary := make(map[string]bool, len(have))
	for _, o := range have {
		onPrimary[o.Name] = true
	}
	var stale []types.ObjectInfo
	for _, o := range backupObjectsOnly(there) {
		if !onPrimary[o.Name] {
			stale = append(stale, o)
		}
	}
	sortForPrune(stale)
	for _, o := range stale {
		if err := mirror.Delete(ctx, o.Name); err != nil && !errors.Is(err, types.ErrObjectNotExist) {
			mr.Failed = append(mr.Failed, fmt.Sprintf("prune %s: %v", o.Name, err))
			continue
		}
		log.Printf("[sync] pruned %s from %s (no longer on the primary)", o.Name, mirror.Location())
		mr.Pruned++
	}
	return mr
}

// sameObject decides whether the mirror already has o. Large objects are
// compared by size; small ones by content, because a sidecar rewrite can keep
// its length.
func sameObject(ctx context.Context, primary, mirror interfaces.Storer, o types.ObjectInfo, onMirror map[string]types.ObjectInfo) (bool, error) {
	m, ok := onMirror[o.Name]
	if !ok {
		return false, nil
	}
	if m.Size != o.Size {
		return false, nil
	}
	if o.Size > smallObjectLimit {
		return true, nil
	}
	a, err := interfaces.GetObject(ctx, primary, o.Name)
	if err != nil {
		return false, err
	}
	b, err := interfaces.GetObject(ctx, mirror, o.Name)
	if err != nil {
		if errors.Is(err, types.ErrObjectNotExist) {
			return false, nil
		}
		return false, err
	}
	return bytes.Equal(a, b), nil
}

// copyObject streams name from src to dst. An archive is hashed on the way
// and checked against its .sha256 when the primary has one; a mismatch
// aborts the copy, because replicating a corrupt archive would make the
// offsite copy as useless as the local one while looking complete.
func copyObject(ctx context.Context, src, dst interfaces.Storer, name string) (int64, error) {
	wantSHA := ""
	if isArchiveName(name) {
		sum, ok, err := readChecksumHex(ctx, src, checksumName(name))
		if err != nil {
			return 0, err
		}
		if ok {
			wantSHA = sum
		}
	}

	r, size, err := src.Open(ctx, name)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	w, err := dst.Create(ctx, name)
	if err != nil {
		return 0, err
	}
	defer w.Abort()

	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(w, h), r)
	if err != nil {
		return n, err
	}
	if n != size {
		return n, fmt.Errorf("read %d bytes, expected %d", n, size)
	}
	if wantSHA != "" && !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), wantSHA) {
		return n, fmt.Errorf("primary copy is corrupt: SHA-256 does not match its .sha256")
	}
	if err := w.Commit(); err != nil {
		return n, err
	}
	return n, nil
}

// backupObjectsOnly keeps the three kinds of object a backup consists of.
func backupObjectsOnly(objs []types.ObjectInfo) []types.ObjectInfo {
	out := objs[:0:0]
	for _, o := range objs {
		if isArchiveName(o.Name) || isMetaName(o.Name) ||
			(strings.HasPrefix(o.Name, objectPrefix) && strings.HasSuffix(o.Name, archiveSuffix+checksumSuffix)) {
			out = append(out, o)
		}
	}
	return out
}

// sortForCopy puts every archive and checksum before every sidecar.
func sortForCopy(objs []types.ObjectInfo) {
	sort.SliceStable(objs, func(i, j int) bool {
		mi, mj := isMetaName(objs[i].Name), isMetaName(objs[j].Name)
		if mi != mj {
			return !mi
		}
		return objs[i].Name < objs[j].Name
	})
}

// sortForPrune is the reverse: sidecars go first.
func sortForPrune(objs []types.ObjectInfo) {
	sort.SliceStable(objs, func(i, j int) bool {
		mi, mj := isMetaName(objs[i].Name), isMetaName(objs[j].Name)
		if mi != mj {
			return mi
		}
		return objs[i].Name < objs[j].Name
	})
}
