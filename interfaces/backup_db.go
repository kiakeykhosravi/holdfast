package interfaces

import (
	"archive/zip"
	"context"
	"holdfast/types"
)

type BackupDB interface {
	Name() string
	SupportedTypes() []types.BackupType
	Supports(t types.BackupType) bool
	ValidateRequest(req types.BackupRequest) error
	ShouldRun(ctx context.Context, req types.BackupRequest) (bool, string, error)
	DumpToArchive(ctx context.Context, zw *zip.Writer, req types.BackupRequest) (*types.DumpOutcome, error)
	ValidateChain(ctx context.Context, chain []types.ExtractedBackup) error
}
