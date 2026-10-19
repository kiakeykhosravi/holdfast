package api

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func Unzip(zipPath, outDir string) ([]string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	if err := os.MkdirAll(outDir, 0o700); err != nil {
		return nil, err
	}

	var extracted []string
	for _, f := range r.File {
		// Prevent path traversal.
		cleanName := filepath.Clean(f.Name)
		if strings.Contains(cleanName, "..") || filepath.IsAbs(cleanName) {
			return nil, fmt.Errorf("invalid zip entry: %q", f.Name)
		}
		outPath := filepath.Join(outDir, cleanName)

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(outPath, 0o700); err != nil {
				return nil, err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(outPath), 0o700); err != nil {
			return nil, err
		}
		src, err := f.Open()
		if err != nil {
			return nil, err
		}
		dst, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			_ = src.Close()
			return nil, err
		}
		_, copyErr := io.Copy(dst, src)
		_ = src.Close()
		_ = dst.Close()
		if copyErr != nil {
			return nil, copyErr
		}
		extracted = append(extracted, outPath)
	}
	return extracted, nil
}
