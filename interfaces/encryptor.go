package interfaces

import "io"

type Encryptor interface {
	Name() string

	EncryptStream(in io.Reader, out io.Writer, passPhrase string) (string, error)

	// DecryptStream decrypts exactly size bytes of in into out. The size is
	// needed up front because the HMAC trailer sits at the end of the stream
	// and the reader may not be seekable (an object store body is not).
	//
	// Plaintext reaches out before the MAC has been verified, so a caller must
	// treat whatever it wrote as poisoned until this returns nil. Decrypt is
	// the file form that does that bookkeeping.
	DecryptStream(in io.Reader, size int64, out io.Writer, passPhrase string) error

	// Decrypt is DecryptStream over local files: the output is written to a
	// temp file and renamed into place only after the MAC verifies, so a
	// wrong passphrase never leaves a partial plaintext behind.
	Decrypt(inPath, outPath, passPhrase string) error
}
