package crypto

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

const testPass = "a-passphrase-of-adequate-length"

// TestRoundTrip is the guarantee the whole tool rests on: what comes back out
// is byte-identical to what went in.
func TestRoundTrip(t *testing.T) {
	e := NewAESEncryptor()
	dir := t.TempDir()

	// Larger than bufSize so the multi-chunk path is exercised, and not a
	// multiple of it so the final short read is too.
	plain := make([]byte, (2*bufSize)+1337)
	if _, err := rand.Read(plain); err != nil {
		t.Fatal(err)
	}

	encPath := filepath.Join(dir, "a.enc")
	out, err := os.Create(encPath)
	if err != nil {
		t.Fatal(err)
	}
	sum, err := e.EncryptStream(bytes.NewReader(plain), out, testPass)
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	if len(sum) != 64 {
		t.Fatalf("checksum is %d hex chars, want 64", len(sum))
	}

	decPath := filepath.Join(dir, "a.zip")
	if err := e.Decrypt(encPath, decPath, testPass); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(decPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round trip changed the data: got %d bytes, want %d", len(got), len(plain))
	}
}

// TestWrongPassphraseLeavesNoPlaintext checks the property that makes the
// decrypt-to-temp-then-rename dance worthwhile: a failed decrypt must not leave
// a partial plaintext of a database backup lying around.
func TestWrongPassphraseLeavesNoPlaintext(t *testing.T) {
	e := NewAESEncryptor()
	dir := t.TempDir()

	plain := make([]byte, 3*bufSize)
	if _, err := rand.Read(plain); err != nil {
		t.Fatal(err)
	}
	encPath := filepath.Join(dir, "a.enc")
	out, err := os.Create(encPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.EncryptStream(bytes.NewReader(plain), out, testPass); err != nil {
		t.Fatal(err)
	}
	out.Close()

	decPath := filepath.Join(dir, "a.zip")
	if err := e.Decrypt(encPath, decPath, "a-completely-different-passphrase"); err == nil {
		t.Fatal("expected a MAC failure with the wrong passphrase")
	}

	if _, err := os.Stat(decPath); err == nil {
		t.Error("the output file exists after a failed decrypt")
	}
	if _, err := os.Stat(decPath + ".tmp"); err == nil {
		t.Error("the temp file was left behind after a failed decrypt")
	}
	// Nothing but the ciphertext should remain.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, en := range entries {
		if en.Name() != "a.enc" {
			t.Errorf("unexpected leftover file %q", en.Name())
		}
	}
}

// TestTamperedCiphertextIsRejected confirms encrypt-then-MAC actually catches a
// flipped bit, which is the difference between a restore that fails and one
// that silently produces corrupt data.
func TestTamperedCiphertextIsRejected(t *testing.T) {
	e := NewAESEncryptor()
	dir := t.TempDir()

	encPath := filepath.Join(dir, "a.enc")
	out, err := os.Create(encPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.EncryptStream(bytes.NewReader([]byte("some backup contents")), out, testPass); err != nil {
		t.Fatal(err)
	}
	out.Close()

	raw, err := os.ReadFile(encPath)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a bit inside the ciphertext, past the header and before the tag.
	victim := len(magicHeader) + saltLen + ivLen + 2
	raw[victim] ^= 0x01
	if err := os.WriteFile(encPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := e.Decrypt(encPath, filepath.Join(dir, "a.zip"), testPass); err == nil {
		t.Fatal("a flipped ciphertext bit was not detected")
	}
}

// TestTruncatedFileIsRejected covers the shape a crashed or ENOSPC-killed write
// leaves behind.
func TestTruncatedFileIsRejected(t *testing.T) {
	e := NewAESEncryptor()
	dir := t.TempDir()
	encPath := filepath.Join(dir, "a.enc")
	if err := os.WriteFile(encPath, []byte("DBBK1short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := e.Decrypt(encPath, filepath.Join(dir, "a.zip"), testPass); err == nil {
		t.Fatal("expected a truncated file to be rejected")
	}
}
