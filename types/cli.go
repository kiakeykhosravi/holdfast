package types

import (
	"fmt"
	"strings"
)

type RunMode string

const (
	RunModeBackup        RunMode = "backup"
	RunModeDecrypt       RunMode = "decrypt"
	RunModeValidateOnly  RunMode = "validate-only"
	RunModeRetentionOnly RunMode = "retention-only"
	RunModeDryRun        RunMode = "dry-run"
	RunModeSyncMirrors   RunMode = "sync-mirrors"
)

// CLIOptions is a parsed representation of the supported CLI flags.
//
// Note: We keep this in types so both main and api can share it without
// dragging flag-parsing into other packages.
type CLIOptions struct {
	ConfigPath            string
	Mode                  RunMode
	DecryptIn             string
	DecryptOut            string
	TestOnly              bool
	TestFile              string
	RetentionOnly         bool
	RetentionDaysOverride *int
	DryRun                bool
	SyncMirrors           bool
	Prune                 bool
	// Source selects one configured source by name. Empty means every
	// enabled source for -sync-mirrors, and is an error for the other modes
	// when more than one source is configured.
	Source     string
	BackupType string
	ParentID   string
}

// InferMode validates mutually exclusive flag combos and derives Mode.
func (o *CLIOptions) InferMode() error {
	active := 0
	if o.DecryptIn != "" {
		o.Mode = RunModeDecrypt
		active++
	}
	if o.TestOnly {
		o.Mode = RunModeValidateOnly
		active++
	}
	if o.RetentionOnly {
		o.Mode = RunModeRetentionOnly
		active++
	}
	if o.DryRun {
		o.Mode = RunModeDryRun
		active++
	}
	if o.SyncMirrors {
		o.Mode = RunModeSyncMirrors
		active++
	}
	if active > 1 {
		return fmt.Errorf("flags -decrypt, -test-only, -retention-only, -dry-run and -sync-mirrors are mutually exclusive")
	}
	if o.Prune && !o.SyncMirrors {
		return fmt.Errorf("-prune only applies to -sync-mirrors")
	}
	if active == 0 {
		o.Mode = RunModeBackup
	}
	return nil
}

func (c Config) ValidateForMode(opts CLIOptions) map[string]string {
	switch opts.Mode {
	case RunModeBackup:
		return c.Validate()

	case RunModeDecrypt:
		err := map[string]string{}
		if len(strings.TrimSpace(c.EncryptPassphrase)) < minPasswordLength {
			err["EncryptPassphrase"] = fmt.Sprintf("EncryptPassphrase too short( min %d characters)", minPasswordLength)
		}
		if strings.TrimSpace(opts.DecryptIn) == "" {
			err["DecryptIn"] = "-decrypt is required in decrypt mode"
		}
		return err

	case RunModeValidateOnly:
		err := map[string]string{}
		if len(strings.TrimSpace(c.EncryptPassphrase)) < minPasswordLength {
			err["EncryptPassphrase"] = fmt.Sprintf("EncryptPassphrase too short( min %d characters)", minPasswordLength)
		}
		if strings.TrimSpace(c.PGTestDockerImage) == "" {
			err["PGTestDockerImage"] = "PGTestDockerImage is required"
		}
		if c.RestoreTimeout < 0 {
			err["RestoreTimeout"] = "RestoreTimeout cannot be less than 0"
		}
		// -file names an object in the configured storage, so the storage
		// must be usable whether or not it is given.
		c.validateStorage(err)
		return err

	case RunModeRetentionOnly:
		err := map[string]string{}
		c.validateStorage(err)
		c.validateRetention(err)
		return err

	case RunModeDryRun:
		return c.Validate()

	case RunModeSyncMirrors:
		err := map[string]string{}
		c.validateStorage(err)
		if len(c.Storage.Mirrors) == 0 {
			err["Storage.Mirrors"] = "storage.mirrors must name at least one mirror to sync to (e.g. [s3])"
		}
		return err

	default:
		return map[string]string{"Mode": "unknown run mode"}
	}
}
