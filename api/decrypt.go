package api

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"holdfast/interfaces"
	"holdfast/types"
)

// DecryptEncryptedBackup verifies an archive's checksum and decrypts it.
//
// in is either a local file path or the name of an archive in the store. A
// path that exists on disk wins: an operator who copied an archive out of a
// bucket, or who is working from a mounted backup_dir, names the file. Anything
// else is looked up in the store by object name, which is how a bucket-held
// archive is decrypted without a separate download step.
//
// When outPath is empty the plaintext goes into the current working directory,
// not next to the archive. backup_dir is commonly a shared or NFS mount, and
// the old default quietly dropped a cluster-sized plaintext dump there with
// the same visibility as the encrypted files around it.
func DecryptEncryptedBackup(ctx context.Context, store interfaces.Storer, encryptor interfaces.Encryptor, in, outPath, passphrase string) (*types.DecryptResult, error) {

	in = strings.TrimSpace(in)
	outPath = strings.TrimSpace(outPath)
	passphrase = strings.TrimSpace(passphrase)

	if in == "" {
		return nil, fmt.Errorf("-decrypt requires a non-empty input")
	}
	if passphrase == "" {
		return nil, fmt.Errorf("encrypt passphrase is required")
	}
	if outPath == "" {
		base := filepath.Base(in)
		name := strings.TrimSuffix(base, ".enc")
		if name == base {
			name = base + ".decrypted.zip"
		}
		cwd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("-out not given and the working directory is unavailable: %w", err)
		}
		outPath = filepath.Join(cwd, name)
	}
	if _, err := os.Stat(outPath); err == nil {
		return nil, fmt.Errorf("refusing to overwrite existing %s; pass -out to choose another path", outPath)
	}

	if fi, err := os.Stat(in); err == nil && !fi.IsDir() {
		return decryptLocalFile(encryptor, in, outPath, passphrase)
	}
	if strings.ContainsAny(in, `/\`) {
		return nil, fmt.Errorf("%s is neither a local file nor an object name in %s", in, store.Location())
	}
	return decryptStoredObject(ctx, store, encryptor, in, outPath, passphrase)
}

func decryptLocalFile(encryptor interfaces.Encryptor, inPath, outPath, passphrase string) (*types.DecryptResult, error) {
	checksumFile := inPath + checksumSuffix
	checksumOK := (*bool)(nil)

	b, err := os.ReadFile(checksumFile)
	switch {
	case err == nil:
		hexSum, _, perr := parseChecksumHex(checksumFile, b)
		if perr != nil {
			return nil, perr
		}
		ok, verr := verifyFileSHA256(inPath, hexSum)
		if verr != nil {
			return nil, verr
		}
		checksumOK = &ok
		if !ok {
			return nil, fmt.Errorf("checksum mismatch for %s", inPath)
		}
	case !os.IsNotExist(err):
		return nil, err
	}

	if err := encryptor.Decrypt(inPath, outPath, passphrase); err != nil {
		return nil, err
	}
	return decryptResult(inPath, outPath, checksumFile, checksumOK), nil
}

// decryptStoredObject reads the object once: the ciphertext hash is computed
// on the same pass as the decrypt, and the plaintext is kept only if both the
// MAC and the recorded checksum agree.
func decryptStoredObject(ctx context.Context, store interfaces.Storer, encryptor interfaces.Encryptor, name, outPath, passphrase string) (*types.DecryptResult, error) {
	sumName := checksumName(name)
	hexSum, hasSum, err := readChecksumHex(ctx, store, sumName)
	if err != nil {
		return nil, err
	}
	if err := decryptObject(ctx, store, encryptor, name, outPath, passphrase, hexSum); err != nil {
		if errors.Is(err, types.ErrObjectNotExist) {
			return nil, fmt.Errorf("%s: not a local file, and %w", name, err)
		}
		return nil, err
	}
	var checksumOK *bool
	if hasSum {
		ok := true // decryptObject fails on a mismatch
		checksumOK = &ok
	}
	return decryptResult(ObjectLocation(store, name), outPath, ObjectLocation(store, sumName), checksumOK), nil
}

func decryptResult(input, outPath, checksumFile string, checksumOK *bool) *types.DecryptResult {
	var bytes int64
	if st, err := os.Stat(outPath); err == nil {
		bytes = st.Size()
	}
	return &types.DecryptResult{
		Mode:         string(types.RunModeDecrypt),
		Input:        input,
		Output:       outPath,
		ChecksumFile: checksumFile,
		ChecksumOK:   checksumOK,
		BytesWritten: bytes,
		CreatedAt:    time.Now(),
	}
}
