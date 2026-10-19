package types

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Medium is how a backup reads the data: through a SQL connection, or from the
// cluster's files on disk.
type Medium string

const (
	MediumLogical  Medium = "logical"
	MediumPhysical Medium = "physical"
)

func (m Medium) Valid() bool {
	return m == MediumLogical || m == MediumPhysical
}

// Level is whether a backup stands alone or builds on a parent.
type Level string

const (
	LevelFull        Level = "full"
	LevelIncremental Level = "incremental"
)

func (l Level) Valid() bool {
	return l == LevelFull || l == LevelIncremental
}

// BackupType is the (medium, level) pair describing what an invocation
// produces. Its wire form is "<medium>-<level>", e.g. "physical-incremental".
//
// The two axes are kept independent on purpose. Which *combinations* are real
// is an engine question, not a shared one: Postgres has no incremental logical
// dump, but MySQL does (mysqldump plus binlog replay), and MongoDB has its own
// answer again. Enumerating a closed set here would bake one engine's
// limitations into the package every engine must import. Ask the driver via
// Supports instead.
type BackupType struct {
	Medium Medium
	Level  Level
}

// DefaultBackupType is what an invocation gets when neither config nor CLI
// specifies one. It matches the behaviour of every existing deployment.
var DefaultBackupType = BackupType{Medium: MediumLogical, Level: LevelFull}

// Convenience constructors for the combinations this project uses today. These
// are shorthand, not an exhaustive list — constructing BackupType directly is
// equally valid and is how a new engine adds its own.
var (
	LogicalFull         = BackupType{Medium: MediumLogical, Level: LevelFull}
	PhysicalFull        = BackupType{Medium: MediumPhysical, Level: LevelFull}
	PhysicalIncremental = BackupType{Medium: MediumPhysical, Level: LevelIncremental}
)

func (b BackupType) String() string {
	return string(b.Medium) + "-" + string(b.Level)
}

func (b BackupType) IsZero() bool {
	return b.Medium == "" && b.Level == ""
}

func (b BackupType) IsFull() bool { return b.Level == LevelFull }

func (b BackupType) RequiresParent() bool { return b.Level == LevelIncremental }

func (b BackupType) Validate() error {
	if !b.Medium.Valid() {
		return fmt.Errorf("unknown backup medium %q (want %q or %q)", b.Medium, MediumLogical, MediumPhysical)
	}
	if !b.Level.Valid() {
		return fmt.Errorf("unknown backup level %q (want %q or %q)", b.Level, LevelFull, LevelIncremental)
	}
	return nil
}

func ParseBackupType(s string) (BackupType, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return DefaultBackupType, nil
	}
	medium, level, found := strings.Cut(s, "-")
	if !found {
		return BackupType{}, fmt.Errorf("invalid backup type %q: want <medium>-<level>, e.g. %q", s, PhysicalIncremental)
	}
	b := BackupType{Medium: Medium(medium), Level: Level(level)}
	if err := b.Validate(); err != nil {
		return BackupType{}, fmt.Errorf("invalid backup type %q: %w", s, err)
	}
	return b, nil
}

func (b BackupType) MarshalJSON() ([]byte, error) {
	return []byte(`"` + b.String() + `"`), nil
}

func (b *BackupType) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	parsed, err := ParseBackupType(s)
	if err != nil {
		return err
	}
	*b = parsed
	return nil
}

func (b BackupType) MarshalYAML() (any, error) {
	return b.String(), nil
}

func (b *BackupType) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := ParseBackupType(s)
	if err != nil {
		return fmt.Errorf("line %d: %w", value.Line, err)
	}
	*b = parsed
	return nil
}
