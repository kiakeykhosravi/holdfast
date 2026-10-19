package types

import (
	"encoding/json"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestParseBackupType(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    BackupType
		wantErr bool
	}{
		{name: "empty falls back to default", in: "", want: DefaultBackupType},
		{name: "whitespace only falls back to default", in: "   ", want: DefaultBackupType},
		{name: "logical full", in: "logical-full", want: LogicalFull},
		{name: "physical full", in: "physical-full", want: PhysicalFull},
		{name: "physical incremental", in: "physical-incremental", want: PhysicalIncremental},
		{
			// Not a combination Postgres can produce, but structurally valid and
			// real for MySQL. types must not reject it; the driver does.
			name: "logical incremental parses",
			in:   "logical-incremental",
			want: BackupType{Medium: MediumLogical, Level: LevelIncremental},
		},
		{name: "case insensitive", in: "  Physical-FULL  ", want: PhysicalFull},
		{name: "missing separator", in: "physical", wantErr: true},
		{name: "unknown medium", in: "magical-full", wantErr: true},
		{name: "unknown level", in: "physical-differential", wantErr: true},
		{name: "reversed axes", in: "full-physical", wantErr: true},
		{name: "empty medium", in: "-full", wantErr: true},
		{name: "empty level", in: "physical-", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBackupType(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseBackupType(%q) = %v, want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseBackupType(%q) unexpected error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParseBackupType(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestBackupTypeRoundTrip(t *testing.T) {
	for _, bt := range []BackupType{LogicalFull, PhysicalFull, PhysicalIncremental} {
		t.Run(bt.String(), func(t *testing.T) {
			jsonBytes, err := json.Marshal(bt)
			if err != nil {
				t.Fatalf("json.Marshal: %v", err)
			}
			if want := `"` + bt.String() + `"`; string(jsonBytes) != want {
				t.Errorf("json.Marshal = %s, want %s", jsonBytes, want)
			}
			var fromJSON BackupType
			if err := json.Unmarshal(jsonBytes, &fromJSON); err != nil {
				t.Fatalf("json.Unmarshal: %v", err)
			}
			if fromJSON != bt {
				t.Errorf("json round trip = %v, want %v", fromJSON, bt)
			}

			yamlBytes, err := yaml.Marshal(bt)
			if err != nil {
				t.Fatalf("yaml.Marshal: %v", err)
			}
			var fromYAML BackupType
			if err := yaml.Unmarshal(yamlBytes, &fromYAML); err != nil {
				t.Fatalf("yaml.Unmarshal: %v", err)
			}
			if fromYAML != bt {
				t.Errorf("yaml round trip = %v, want %v", fromYAML, bt)
			}
		})
	}
}

// The config field is the reason UnmarshalYAML exists: a typo must fail at
// parse time, not halfway through a cluster-sized dump.
func TestBackupTypeYAMLInStruct(t *testing.T) {
	var cfg struct {
		BackupType BackupType `yaml:"backup_type"`
	}
	if err := yaml.Unmarshal([]byte("backup_type: physical-incremental\n"), &cfg); err != nil {
		t.Fatalf("unmarshal valid: %v", err)
	}
	if cfg.BackupType != PhysicalIncremental {
		t.Errorf("got %v, want %v", cfg.BackupType, PhysicalIncremental)
	}
	if err := yaml.Unmarshal([]byte("backup_type: physical-diferential\n"), &cfg); err == nil {
		t.Error("unmarshal invalid: want error, got nil")
	}
}

// omitzero, not omitempty: omitempty is a no-op on struct fields, so getting
// this wrong silently changes the stdout contract for every existing run.
func TestBackupResultOmitsUnsetType(t *testing.T) {
	b, err := json.Marshal(BackupResult{ID: "2026-08-25_03-00-00"})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	for _, key := range []string{"backupType", "parentID", "chainID"} {
		if _, present := got[key]; present {
			t.Errorf("unset %s should be omitted, got %v", key, got[key])
		}
	}
	if _, present := got["id"]; !present {
		t.Error("id should always be present")
	}
}

func TestBackupRequestValidate(t *testing.T) {
	validParent := &ParentRef{
		ID:        "2026-08-24_03-00-00",
		Type:      PhysicalFull,
		StatePath: "/var/lib/holdfast/manifests/2026-08-24_03-00-00",
	}

	tests := []struct {
		name    string
		req     BackupRequest
		wantErr bool
	}{
		{name: "full without parent", req: BackupRequest{Type: PhysicalFull}},
		{name: "incremental with parent", req: BackupRequest{Type: PhysicalIncremental, Parent: validParent}},
		{
			// Structurally fine. The Postgres driver rejects it; types does not.
			name: "incremental rooted in a logical full",
			req: BackupRequest{
				Type:   PhysicalIncremental,
				Parent: &ParentRef{ID: "x", Type: LogicalFull, StatePath: "/tmp/x"},
			},
		},
		{name: "zero type", req: BackupRequest{}, wantErr: true},
		{name: "incremental without parent", req: BackupRequest{Type: PhysicalIncremental}, wantErr: true},
		{name: "full with parent", req: BackupRequest{Type: PhysicalFull, Parent: validParent}, wantErr: true},
		{
			name:    "parent missing ID",
			req:     BackupRequest{Type: PhysicalIncremental, Parent: &ParentRef{Type: PhysicalFull, StatePath: "/tmp/x"}},
			wantErr: true,
		},
		{
			name:    "parent missing state path",
			req:     BackupRequest{Type: PhysicalIncremental, Parent: &ParentRef{ID: "x", Type: PhysicalFull}},
			wantErr: true,
		},
		{
			name:    "parent with unparseable type",
			req:     BackupRequest{Type: PhysicalIncremental, Parent: &ParentRef{ID: "x", Type: BackupType{Medium: "magical", Level: LevelFull}, StatePath: "/tmp/x"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if tt.wantErr && err == nil {
				t.Error("want error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
