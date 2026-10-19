package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"holdfast/interfaces"
	"holdfast/types"
)

const (
	archiveSuffix  = ".zip.enc"
	checksumSuffix = ".sha256"
	metaSuffix     = ".meta.json"
	objectPrefix   = "backup_"
)

// archiveName is the object name of a backup's archive.
func archiveName(id string) string { return objectPrefix + id + archiveSuffix }

// checksumName is the object name of an archive's .sha256.
func checksumName(archive string) string { return archive + checksumSuffix }

// idFromArchiveName inverts archiveName. It returns "" for anything else.
func idFromArchiveName(name string) string {
	if !strings.HasPrefix(name, objectPrefix) || !strings.HasSuffix(name, archiveSuffix) {
		return ""
	}
	return strings.TrimSuffix(strings.TrimPrefix(name, objectPrefix), archiveSuffix)
}

func isArchiveName(name string) bool { return idFromArchiveName(name) != "" }

func isMetaName(name string) bool {
	return strings.HasPrefix(name, objectPrefix) && strings.HasSuffix(name, metaSuffix)
}

// ObjectLocation is a human-readable address for an object, for results and
// log lines: a path for the file backend, an s3:// URL for S3.
func ObjectLocation(s interfaces.Storer, name string) string {
	loc := s.Location()
	if strings.HasSuffix(loc, "/") {
		return loc + name
	}
	return loc + "/" + name
}

var sha256Re = regexp.MustCompile(`(?i)[0-9a-f]{64}`)

// readChecksumHex extracts a 64-hex SHA256 string from a checksum object.
//
// It accepts either just the hex string, or a standard "sha256sum" line (hex,
// whitespace, filename). ok=false means the object does not exist.
func readChecksumHex(ctx context.Context, store interfaces.Storer, name string) (hexSum string, ok bool, err error) {
	b, err := interfaces.GetObject(ctx, store, name)
	if err != nil {
		if errors.Is(err, types.ErrObjectNotExist) {
			return "", false, nil
		}
		return "", false, err
	}
	return parseChecksumHex(name, b)
}

func parseChecksumHex(name string, b []byte) (string, bool, error) {
	m := sha256Re.Find(b)
	if m == nil {
		return "", true, fmt.Errorf("checksum %s does not contain a SHA256 hash", name)
	}
	return strings.ToLower(string(m)), true, nil
}

// verifyObjectSHA256 streams an object through SHA-256 and compares.
func verifyObjectSHA256(ctx context.Context, store interfaces.Storer, name, expectedHex string) (bool, error) {
	r, _, err := store.Open(ctx, name)
	if err != nil {
		return false, err
	}
	defer r.Close()
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return false, err
	}
	return strings.EqualFold(hex.EncodeToString(h.Sum(nil)), expectedHex), nil
}

// verifyFileSHA256 is verifyObjectSHA256 for a local file.
func verifyFileSHA256(path, expectedHex string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}
	return strings.EqualFold(hex.EncodeToString(h.Sum(nil)), expectedHex), nil
}

// findLatestArchive returns the name of the most recently modified archive.
func findLatestArchive(ctx context.Context, store interfaces.Storer) (string, error) {
	objs, err := store.List(ctx, objectPrefix)
	if err != nil {
		return "", err
	}
	var best types.ObjectInfo
	for _, o := range objs {
		if !isArchiveName(o.Name) {
			continue
		}
		if best.Name == "" || o.ModTime.After(best.ModTime) {
			best = o
		}
	}
	if best.Name == "" {
		return "", fmt.Errorf("no encrypted backups found in %s", store.Location())
	}
	return best.Name, nil
}

// decryptObject streams an object from the store through the encryptor into
// outPath on local disk. The plaintext goes to outPath+".tmp" and is renamed
// into place only once the MAC verifies and, when wantSHA is given, the
// ciphertext's SHA-256 matches. One read of the object serves both checks,
// which matters when the object is a cluster-sized archive in a bucket.
func decryptObject(
	ctx context.Context,
	store interfaces.Storer,
	encryptor interfaces.Encryptor,
	name, outPath, passphrase, wantSHA string,
) error {
	r, size, err := store.Open(ctx, name)
	if err != nil {
		return err
	}
	defer r.Close()

	h := sha256.New()
	src := io.TeeReader(r, h)

	tmp := outPath + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := encryptor.DecryptStream(src, size, out, passphrase); err != nil {
		return fail(fmt.Errorf("decrypt %s: %w", name, err))
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if wantSHA != "" && !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), wantSHA) {
		os.Remove(tmp)
		return fmt.Errorf("checksum mismatch for %s", name)
	}
	if err := os.Rename(tmp, outPath); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// countingWriter counts what passes through it.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
