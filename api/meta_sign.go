package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/scrypt"

	"holdfast/types"
)

// The sidecar is plaintext and lives in backup_dir, which is frequently a
// shared mount. Before signing existed anyone who could write there could
// rewrite parentID or chainID and steer retention into deleting a chain that
// was still needed, or point an incremental's lineage at an unrelated archive.
// The archive itself was authenticated by its HMAC; the index describing it
// was not.
//
// A signature is an HMAC-SHA256 over the sidecar's identity fields, keyed by a
// value derived from the backup passphrase. It is optional on read so that
// sidecars written before this existed keep working, and so that a
// -retention-only run on a host with no passphrase can still operate (with a
// warning). A sidecar that carries a signature that does not verify is treated
// like one that does not parse: unknown dependencies, do not touch.

// metaKeySalt is fixed on purpose. scrypt's cost is what protects the
// passphrase from being brute-forced through a signature, and a per-file salt
// would cost one scrypt per sidecar read, which does not scale to a directory
// scan.
const metaKeySalt = "holdfast/meta-signature/v1"

var (
	metaKeyMu    sync.Mutex
	metaKeyCache = map[string][]byte{}
)

// MetaKey derives the sidecar signing key from the passphrase. An empty
// passphrase yields nil, which disables signing and verification.
//
// scrypt with the archive's parameters costs about a tenth of a second, so the
// result is memoised for the life of the process.
func MetaKey(passphrase string) []byte {
	if passphrase == "" {
		return nil
	}
	metaKeyMu.Lock()
	defer metaKeyMu.Unlock()
	if k, ok := metaKeyCache[passphrase]; ok {
		return k
	}
	k, err := scrypt.Key([]byte(passphrase), []byte(metaKeySalt), 1<<15, 8, 1, 32)
	if err != nil {
		// scrypt only fails on invalid parameters, which are constants here.
		panic(fmt.Sprintf("derive sidecar key: %v", err))
	}
	metaKeyCache[passphrase] = k
	return k
}

// metaKeyFor is the key a given config signs with.
func metaKeyFor(cfg types.Config) []byte { return MetaKey(cfg.EncryptPassphrase) }

// signMeta computes the signature for a sidecar.
func signMeta(key []byte, m types.BackupMeta) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(canonicalMeta(m)))
	return hex.EncodeToString(mac.Sum(nil))
}

// verifyMeta reports whether the sidecar's signature matches its contents.
func verifyMeta(key []byte, m types.BackupMeta) bool {
	want, err := hex.DecodeString(m.Signature)
	if err != nil {
		return false
	}
	got, _ := hex.DecodeString(signMeta(key, m))
	return hmac.Equal(want, got)
}

// canonicalMeta is the byte string the signature covers. Every field that
// influences chain walking, retention, or parent selection is included, and
// the field order is part of the format: changing it invalidates every
// existing signature, so it belongs with a MetaSchemaVersion bump.
func canonicalMeta(m types.BackupMeta) string {
	validatedAt := ""
	if m.ValidatedAt != nil {
		validatedAt = m.ValidatedAt.UTC().Format(time.RFC3339Nano)
	}
	fields := []string{
		fmt.Sprint(m.SchemaVersion),
		m.ID,
		m.DBType,
		m.Type.String(),
		m.ChainID,
		m.ParentID,
		m.CreatedAt.UTC().Format(time.RFC3339Nano),
		m.EncryptedFile,
		fmt.Sprint(m.SizeBytes),
		m.ChecksumSHA256,
		fmt.Sprint(m.EntryCount),
		m.Encryption.Container,
		m.Encryption.KDF,
		m.PositionStart,
		m.PositionEnd,
		fmt.Sprint(m.Timeline),
		validatedAt,
		string(m.ValidationResult),
	}
	// A field containing the separator could make two different sidecars
	// canonicalise identically, so each field is length-prefixed.
	var b strings.Builder
	for _, f := range fields {
		fmt.Fprintf(&b, "%d:%s\n", len(f), f)
	}
	return b.String()
}
