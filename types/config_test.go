package types

import "testing"

func baseConfig() Config {
	return Config{
		BackupDir:         "/b",
		StateDir:          "/s",
		EncryptPassphrase: "a-passphrase-of-adequate-length",
		RunOnClusterRole:  "primary",
		PGConnString:      "postgres://u:p@h:5432/db",
		PGDumpPath:        "pg_dump",
		PGDumpAllPath:     "pg_dumpall",
		PGTestDockerImage: "postgres:18",
		BackupType:        DefaultBackupType,
	}
}

func TestStorageValidation(t *testing.T) {
	cases := []struct {
		name    string
		mut     func(*Config)
		wantKey string // "" means valid
	}{
		{"file default is valid", func(c *Config) {}, ""},
		{"file needs backup_dir", func(c *Config) { c.BackupDir = "" }, "BackupDir"},
		{"s3 primary needs bucket", func(c *Config) {
			c.Storage.Primary = StorageS3
			c.Storage.S3.Endpoint = "http://x"
		}, "Storage.S3.Bucket"},
		{"s3 primary needs state_dir", func(c *Config) {
			c.Storage.Primary = StorageS3
			c.Storage.S3 = S3Config{Bucket: "b", Endpoint: "http://x"}
			c.StateDir = ""
		}, "StateDir"},
		{"s3 on AWS needs a region", func(c *Config) {
			c.Storage.Mirrors = []StorageKind{StorageS3}
			c.Storage.S3 = S3Config{Bucket: "b"}
		}, "Storage.S3.Region"},
		{"s3 with an endpoint needs no region", func(c *Config) {
			c.Storage.Mirrors = []StorageKind{StorageS3}
			c.Storage.S3 = S3Config{Bucket: "b", Endpoint: "https://s3.example"}
		}, ""},
		{"mirror cannot repeat the primary", func(c *Config) {
			c.Storage.Mirrors = []StorageKind{StorageFile}
		}, "Storage.Mirrors"},
		{"unknown kind", func(c *Config) { c.Storage.Primary = "tape" }, "Storage.Primary"},
		{"half credentials", func(c *Config) {
			c.Storage.Mirrors = []StorageKind{StorageS3}
			c.Storage.S3 = S3Config{Bucket: "b", Endpoint: "http://x", AccessKeyID: "k"}
		}, "Storage.S3.Credentials"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseConfig()
			tc.mut(&c)
			errs := c.Validate()
			if tc.wantKey == "" {
				if len(errs) != 0 {
					t.Fatalf("want valid, got %v", errs)
				}
				return
			}
			if _, ok := errs[tc.wantKey]; !ok {
				t.Fatalf("want error under %q, got %v", tc.wantKey, errs)
			}
		})
	}
}

// The sync host serves backup_dir and talks to the mirror. It has no
// database, no pg_dump, no passphrase, and must not be asked for them.
func TestSyncModeNeedsOnlyStorage(t *testing.T) {
	c := Config{
		BackupDir: "/nfs/backups",
		Storage: StorageConfig{
			Primary: StorageFile,
			Mirrors: []StorageKind{StorageS3},
			S3:      S3Config{Bucket: "b", Endpoint: "https://s3.example"},
		},
	}
	if errs := c.ValidateForMode(CLIOptions{Mode: RunModeSyncMirrors}); len(errs) != 0 {
		t.Fatalf("sync mode demanded more than storage: %v", errs)
	}
	c.Storage.Mirrors = nil
	if errs := c.ValidateForMode(CLIOptions{Mode: RunModeSyncMirrors}); errs["Storage.Mirrors"] == "" {
		t.Fatalf("sync mode without mirrors must be rejected, got %v", errs)
	}
}

func sourcesConfig() Config {
	c := baseConfig()
	c.Storage.Mirrors = []StorageKind{StorageS3}
	c.Storage.S3 = S3Config{Bucket: "b", Endpoint: "https://s3.example"}
	c.BackupDir = ""
	c.Sources = []SourceConfig{
		{Name: "opb", BackupDir: "/nfs/data/opb_backups"},
		{Name: "pg", BackupDir: "/nfs/data/pg_backups"},
	}
	return c
}

func TestSourcesValidation(t *testing.T) {
	no := false
	cases := []struct {
		name    string
		mut     func(*Config)
		wantKey string
	}{
		{"two sources, distinct", func(c *Config) {}, ""},
		{"top-level backup_dir not needed with sources", func(c *Config) { c.BackupDir = "" }, ""},
		{"duplicate name", func(c *Config) { c.Sources[1].Name = "opb" }, "Sources[1].Name"},
		{"bad name", func(c *Config) { c.Sources[1].Name = "pg/backups" }, "Sources[1].Name"},
		{"duplicate dir", func(c *Config) { c.Sources[1].BackupDir = c.Sources[0].BackupDir }, "Sources[1].BackupDir"},
		{"missing dir", func(c *Config) { c.Sources[1].BackupDir = "" }, "Sources[1].BackupDir"},
		{"prefix collision", func(c *Config) { c.Sources[1].Prefix = "opb" }, "Sources[1].Prefix"},
		{"prefix collision through base prefix", func(c *Config) {
			c.Storage.S3.Prefix = "x"
			c.Sources[0].Prefix = "same"
			c.Sources[1].Prefix = "same/"
		}, "Sources[1].Prefix"},
		{"disabled source still validated", func(c *Config) { c.Sources[1].Enabled = &no; c.Sources[1].BackupDir = "" }, "Sources[1].BackupDir"},
		{"sources need file primary", func(c *Config) { c.Storage.Primary = StorageS3; c.Storage.Mirrors = nil }, "Sources"},
		{"sources need state_dir", func(c *Config) { c.StateDir = "" }, "StateDir"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := sourcesConfig()
			tc.mut(&c)
			errs := c.ValidateForMode(CLIOptions{Mode: RunModeSyncMirrors})
			if tc.wantKey == "" {
				if len(errs) != 0 {
					t.Fatalf("want valid, got %v", errs)
				}
				return
			}
			if _, ok := errs[tc.wantKey]; !ok {
				t.Fatalf("want error under %q, got %v", tc.wantKey, errs)
			}
		})
	}
}

func TestSelectSourcesAndForSource(t *testing.T) {
	no := false
	c := sourcesConfig()
	c.Storage.S3.Prefix = "prod"
	c.Sources = append(c.Sources, SourceConfig{Name: "legacy", BackupDir: "/nfs/data/holdfast", Enabled: &no})

	all, err := c.SelectSources("")
	if err != nil || len(all) != 2 || all[0].Name != "opb" || all[1].Name != "pg" {
		t.Fatalf("SelectSources() = %v, %v; want the two enabled", all, err)
	}
	one, err := c.SelectSources("legacy")
	if err != nil || len(one) != 1 || one[0].Name != "legacy" {
		t.Fatalf("a disabled source must still be selectable by name: %v, %v", one, err)
	}
	if _, err := c.SelectSources("nope"); err == nil {
		t.Fatal("unknown source accepted")
	}

	// Prefix composes base + name, and ForSource yields a plain single config.
	r := c.ForSource(all[0])
	if r.BackupDir != "/nfs/data/opb_backups" || r.Storage.S3.Prefix != "prod/opb" || len(r.Sources) != 0 {
		t.Fatalf("ForSource = dir %q prefix %q sources %d", r.BackupDir, r.Storage.S3.Prefix, len(r.Sources))
	}
	c.Sources[1].Prefix = "/custom/"
	r = c.ForSource(c.Sources[1])
	if r.Storage.S3.Prefix != "prod/custom" {
		t.Fatalf("explicit prefix = %q, want prod/custom", r.Storage.S3.Prefix)
	}

	// No sources block: the implicit single source is backup_dir as-is.
	plain := baseConfig()
	plain.Storage.S3.Prefix = "keep"
	impl, err := plain.SelectSources("")
	if err != nil || len(impl) != 1 {
		t.Fatalf("implicit source: %v, %v", impl, err)
	}
	r = plain.ForSource(impl[0])
	if r.BackupDir != "/b" || r.Storage.S3.Prefix != "keep" {
		t.Fatalf("implicit ForSource changed the config: dir %q prefix %q", r.BackupDir, r.Storage.S3.Prefix)
	}
	if _, err := plain.SelectSources("opb"); err == nil {
		t.Fatal("-source accepted with no sources configured")
	}
}
