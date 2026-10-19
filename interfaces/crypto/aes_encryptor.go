package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"golang.org/x/crypto/scrypt"
	"hash"
	"io"
	"os"
)

const (
	magicHeader = "DBBK1"

	saltLen = 16
	ivLen   = 16
	hmacLen = 32
	keyLen  = 64 // 32 enc + 32 mac
	bufSize = 1024 * 1024

	scryptN = 1 << 15
	scryptR = 8
	scryptP = 1
)

type AESEncryptor struct{}

func NewAESEncryptor() *AESEncryptor {
	return &AESEncryptor{}
}

func (e *AESEncryptor) Name() string {
	return "aes-ctr-hmac"
}

func deriveKeys(passphrase string, salt []byte) (encKey, macKey []byte, err error) {
	keyMaterial, err := scrypt.Key(
		[]byte(passphrase),
		salt,
		scryptN,
		scryptR,
		scryptP,
		keyLen,
	)
	if err != nil {
		return nil, nil, err
	}
	return keyMaterial[:32], keyMaterial[32:], nil
}

func (e *AESEncryptor) EncryptStream(in io.Reader, out io.Writer, passphrase string) (string, error) {
	var fileHash hash.Hash = sha256.New()
	w := io.MultiWriter(out, fileHash)

	salt := make([]byte, saltLen)
	iv := make([]byte, ivLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	if _, err := rand.Read(iv); err != nil {
		return "", err
	}

	encKey, macKey, err := deriveKeys(passphrase, salt)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(encKey)
	if err != nil {
		return "", err
	}

	stream := cipher.NewCTR(block, iv)
	mac := hmac.New(sha256.New, macKey)

	// Write header, salt, iv
	if _, err := w.Write([]byte(magicHeader)); err != nil {
		return "", err
	}
	if _, err := w.Write(salt); err != nil {
		return "", err
	}
	if _, err := w.Write(iv); err != nil {
		return "", err
	}

	mac.Write([]byte(magicHeader))
	mac.Write(salt)
	mac.Write(iv)

	buf := make([]byte, bufSize)
	for {
		n, err := in.Read(buf)
		if n > 0 {
			encrypted := make([]byte, n)
			stream.XORKeyStream(encrypted, buf[:n])
			if _, wErr := w.Write(encrypted); wErr != nil {
				return "", wErr
			}
			mac.Write(encrypted)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}

	tag := mac.Sum(nil)
	if _, err := w.Write(tag); err != nil {
		return "", fmt.Errorf("failed to write HMAC tag: %w", err)
	}

	return hex.EncodeToString(fileHash.Sum(nil)), nil
}

func (e *AESEncryptor) DecryptStream(in io.Reader, size int64, out io.Writer, passphrase string) error {
	minSize := int64(len(magicHeader) + saltLen + ivLen + hmacLen)
	if size < minSize {
		return errors.New("invalid encrypted file")
	}

	header := make([]byte, len(magicHeader)+saltLen+ivLen)
	if _, err := io.ReadFull(in, header); err != nil {
		return err
	}
	if string(header[:len(magicHeader)]) != magicHeader {
		return errors.New("invalid magic header")
	}
	salt := header[len(magicHeader) : len(magicHeader)+saltLen]
	iv := header[len(magicHeader)+saltLen:]

	encKey, macKey, err := deriveKeys(passphrase, salt)
	if err != nil {
		return err
	}
	block, err := aes.NewCipher(encKey)
	if err != nil {
		return err
	}
	stream := cipher.NewCTR(block, iv)
	mac := hmac.New(sha256.New, macKey)
	mac.Write(header)

	cipherLen := size - int64(len(header)) - hmacLen
	buf := make([]byte, bufSize)
	plain := make([]byte, bufSize)
	for cipherLen > 0 {
		n := int64(len(buf))
		if cipherLen < n {
			n = cipherLen
		}
		if _, err := io.ReadFull(in, buf[:n]); err != nil {
			return err
		}
		mac.Write(buf[:n])
		stream.XORKeyStream(plain[:n], buf[:n])
		if _, err := out.Write(plain[:n]); err != nil {
			return fmt.Errorf("failed to write decrypted data: %w", err)
		}
		cipherLen -= n
	}

	tag := make([]byte, hmacLen)
	if _, err := io.ReadFull(in, tag); err != nil {
		return err
	}
	if !hmac.Equal(tag, mac.Sum(nil)) {
		return errors.New("HMAC mismatch: wrong passphrase or corrupted file")
	}
	return nil
}

func (e *AESEncryptor) Decrypt(inPath, outPath, passphrase string) error {
	in, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer in.Close()
	stat, err := in.Stat()
	if err != nil {
		return err
	}

	// Write to a temp file first; only move into place if the MAC verifies.
	tmpOut := outPath + ".tmp"
	out, err := os.OpenFile(tmpOut, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
		_ = os.Remove(tmpOut) // best-effort cleanup if we exit early
	}()

	if err := e.DecryptStream(in, stat.Size(), out, passphrase); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmpOut, outPath)
}
