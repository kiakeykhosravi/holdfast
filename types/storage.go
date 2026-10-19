package types

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrObjectNotExist is returned (wrapped) by a Storer when the named object is
// not there. Callers test for it with errors.Is, never by string matching, and
// never by assuming a backend-specific error: the whole point of the Storer
// seam is that api cannot tell a directory from a bucket.
var ErrObjectNotExist = errors.New("object does not exist")

// ObjectInfo is what a Storer reports about one stored object.
//
// Name is the object's basename within the store — "backup_<id>.zip.enc", never
// a path or a bucket key. Keeping names flat is deliberate: BackupMeta already
// records EncryptedFile as a basename so a backup set survives being moved, and
// a Storer that accepted paths would reintroduce the layout coupling the sidecar
// was designed to avoid.
type ObjectInfo struct {
	Name    string
	Size    int64
	ModTime time.Time
}

// ValidateObjectName rejects anything that is not a plain basename. Both
// backends call it on every operation so a name like "../x" can neither escape
// a directory nor create a nested S3 key.
func ValidateObjectName(name string) error {
	switch {
	case strings.TrimSpace(name) == "":
		return fmt.Errorf("object name is empty")
	case name == "." || name == "..":
		return fmt.Errorf("object name %q is not a file name", name)
	case strings.ContainsAny(name, `/\`):
		return fmt.Errorf("object name %q must not contain path separators", name)
	case strings.ContainsRune(name, 0):
		return fmt.Errorf("object name contains a NUL byte")
	}
	return nil
}
