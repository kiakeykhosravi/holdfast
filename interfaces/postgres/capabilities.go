package postgres

import (
	"fmt"

	"holdfast/types"
)

// SupportedTypes lists the backup types this driver can currently produce.
//
// This list is the feature switch: a config asking for a type absent from it
// fails immediately with a clear message instead of somewhere deep in the dump.
func (p *PostgresBackupDB) SupportedTypes() []types.BackupType {
	return []types.BackupType{
		types.LogicalFull,
		types.PhysicalFull,
		types.PhysicalIncremental,
	}
}

func (p *PostgresBackupDB) Supports(t types.BackupType) bool {
	for _, s := range p.SupportedTypes() {
		if s == t {
			return true
		}
	}
	return false
}

// ValidateRequest applies the engine-specific rules that types.BackupRequest
// deliberately does not.
//
// types only checks structure: does the type parse, is a parent present when
// the level needs one. Which combinations are real, and what may legally be a
// parent, is knowledge about Postgres — and putting it here is what stops a
// future MySQL driver from inheriting Postgres's limitations.
func (p *PostgresBackupDB) ValidateRequest(req types.BackupRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}

	if !p.Supports(req.Type) {
		return fmt.Errorf("postgres cannot produce %s (supported: %s)", req.Type, joinTypes(p.SupportedTypes()))
	}

	// Postgres has no incremental logical dump. pg_dump emits a self-contained
	// snapshot and pg_restore cannot consume a delta, so this combination is
	// not a missing feature — it does not exist. MySQL's binlog replay makes
	// the same combination perfectly valid, which is why the rule lives here
	// and not in types.
	if req.Type.Medium == types.MediumLogical && req.Type.RequiresParent() {
		return fmt.Errorf("postgres has no incremental logical backup: pg_dump cannot emit a delta")
	}

	if req.Parent != nil {
		// An incremental is applied with pg_combinebackup over a chain of
		// basebackups. A logical dump is not a member of such a chain, so
		// rooting one there would fail only after copying the whole cluster.
		if req.Parent.Type.Medium != types.MediumPhysical {
			return fmt.Errorf("cannot take %s against parent %s of type %s: a postgres incremental chain must be rooted in a physical backup",
				req.Type, req.Parent.ID, req.Parent.Type)
		}
	}

	return nil
}

func joinTypes(ts []types.BackupType) string {
	out := ""
	for i, t := range ts {
		if i > 0 {
			out += ", "
		}
		out += t.String()
	}
	return out
}
