package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"holdfast/interfaces"
	"holdfast/types"
)

// WriteState caches a backup's driver state — for Postgres, its
// backup_manifest — so the next incremental can reference it without
// decrypting the parent archive.
//
// The cache is strictly disposable. Losing it costs one decrypt, never
// correctness, which is why a write failure is a warning to the caller rather
// than a failed backup. It is local disk by design: the driver reads it by
// path.
func WriteState(cfg types.Config, id string, state []byte) error {
	if len(state) == 0 {
		return nil
	}
	path := StatePath(cfg, id)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, state, 0o600); err != nil {
		return fmt.Errorf("write state cache: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("finalize state cache: %w", err)
	}
	return nil
}

// EnsureState makes a parent's driver state available on local disk, restoring
// it from the encrypted archive if the cache has been cleared.
//
// This is the fallback path that lets the cache stay disposable: a wiped
// state_dir costs a decrypt of one archive, not a broken chain.
func EnsureState(
	ctx context.Context,
	cfg types.Config,
	store interfaces.Storer,
	encryptor interfaces.Encryptor,
	meta types.BackupMeta,
	stateEntryName string,
) (string, error) {

	path := StatePath(cfg, meta.ID)
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}

	workdir, err := os.MkdirTemp(cfg.ScratchDir, scratchPrefix+"state_")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(workdir)

	zipPath := filepath.Join(workdir, "archive.zip")
	if err := decryptObject(ctx, store, encryptor, meta.EncryptedFile, zipPath, cfg.EncryptPassphrase, ""); err != nil {
		return "", fmt.Errorf("rebuild state cache for %s: %w", meta.ID, err)
	}
	files, err := Unzip(zipPath, filepath.Join(workdir, "extract"))
	if err != nil {
		return "", fmt.Errorf("rebuild state cache for %s: unzip: %w", meta.ID, err)
	}

	for _, f := range files {
		if filepath.Base(f) != stateEntryName {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			return "", err
		}
		if err := WriteState(cfg, meta.ID, b); err != nil {
			return "", err
		}
		return path, nil
	}
	return "", fmt.Errorf("backup %s contains no %q, so it cannot be a parent", meta.ID, stateEntryName)
}
