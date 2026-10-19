package api

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"holdfast/interfaces"
	"holdfast/interfaces/filestore"
	"holdfast/types"
)

// newTestStore is the file-backed Storer over dir. api tests use the file
// backend because it is what every assertion about "the directory" means,
// and because the two backends share one contract (interfaces/storetest).
func newTestStore(t *testing.T, dir string) interfaces.Storer {
	t.Helper()
	s, err := filestore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// metaPath is where the file backend puts a sidecar; tests use it to tamper
// with or inspect the raw file.
func metaPath(dir, id string) string { return filepath.Join(dir, types.MetaFileName(id)) }

// fakeDB is a BackupDB that writes a configurable amount of data into the
// archive. Nothing here talks to a database.
type fakeDB struct {
	entryBytes   int
	dumpErr      error
	validateErr  error
	shouldRunErr error
	shouldRun    bool
	reason       string
}

func newFakeDB() *fakeDB { return &fakeDB{entryBytes: 4 << 20, shouldRun: true, reason: "ok"} }

func (f *fakeDB) Name() string { return "fake" }

func (f *fakeDB) SupportedTypes() []types.BackupType {
	return []types.BackupType{types.LogicalFull, types.PhysicalFull, types.PhysicalIncremental}
}

func (f *fakeDB) Supports(t types.BackupType) bool {
	for _, s := range f.SupportedTypes() {
		if s == t {
			return true
		}
	}
	return false
}

func (f *fakeDB) ValidateRequest(req types.BackupRequest) error { return req.Validate() }

func (f *fakeDB) ShouldRun(ctx context.Context, req types.BackupRequest) (bool, string, error) {
	return f.shouldRun, f.reason, f.shouldRunErr
}

func (f *fakeDB) DumpToArchive(ctx context.Context, zw *zip.Writer, req types.BackupRequest) (*types.DumpOutcome, error) {
	if f.dumpErr != nil {
		return nil, f.dumpErr
	}
	w, err := zw.CreateHeader(&zip.FileHeader{Name: "base.tar", Method: zip.Store, Modified: time.Now()})
	if err != nil {
		return nil, err
	}
	chunk := make([]byte, 1<<20)
	for written := 0; written < f.entryBytes; written += len(chunk) {
		if _, err := w.Write(chunk); err != nil {
			return nil, err
		}
	}
	// The driver-state entry, as a physical backup carries it. EnsureState
	// looks for it when the parent-state cache is cold.
	state := []byte(`{"WAL-Ranges":[]}`)
	mw, err := zw.CreateHeader(&zip.FileHeader{Name: StateEntryName, Method: zip.Store, Modified: time.Now()})
	if err != nil {
		return nil, err
	}
	if _, err := mw.Write(state); err != nil {
		return nil, err
	}
	return &types.DumpOutcome{Entries: []string{"base.tar", StateEntryName}, State: state}, nil
}

func (f *fakeDB) ValidateChain(ctx context.Context, chain []types.ExtractedBackup) error {
	return f.validateErr
}

// failingEncryptor mimics the real EncryptStream's behaviour on a failed output
// write: it returns immediately without draining its input. That is what makes
// the deadlock reachable, so the fake has to reproduce it faithfully.
type failingEncryptor struct {
	afterBytes int
	err        error
}

func (e *failingEncryptor) Name() string { return "failing" }

func (e *failingEncryptor) Decrypt(inPath, outPath, passPhrase string) error {
	return fmt.Errorf("not implemented")
}

func (e *failingEncryptor) DecryptStream(in io.Reader, size int64, out io.Writer, passPhrase string) error {
	return fmt.Errorf("not implemented")
}

func (e *failingEncryptor) EncryptStream(in io.Reader, out io.Writer, passPhrase string) (string, error) {
	buf := make([]byte, 1<<20)
	read := 0
	for {
		n, err := in.Read(buf)
		read += n
		if read >= e.afterBytes {
			return "", e.err // leaves `in` open and undrained, exactly as the real one does
		}
		if err == io.EOF {
			return "sum", nil
		}
		if err != nil {
			return "", err
		}
	}
}

// passthroughEncryptor copies input to output and reports a fixed checksum.
type passthroughEncryptor struct{}

func (passthroughEncryptor) Name() string { return "passthrough" }

func (passthroughEncryptor) Decrypt(inPath, outPath, passPhrase string) error {
	return fmt.Errorf("not implemented")
}

func (passthroughEncryptor) DecryptStream(in io.Reader, size int64, out io.Writer, passPhrase string) error {
	_, err := io.Copy(out, in)
	return err
}

func (passthroughEncryptor) EncryptStream(in io.Reader, out io.Writer, passPhrase string) (string, error) {
	if _, err := io.Copy(out, in); err != nil {
		return "", err
	}
	return "0000000000000000000000000000000000000000000000000000000000000000", nil
}
