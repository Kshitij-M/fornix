package contracts

import (
	"strings"
	"testing"
	"time"
)

func TestQualificationRefreshRequestNormalizesAndHashesDeterministically(t *testing.T) {
	base := QualificationRefreshRequest{
		WorkspaceID: "workspace-a", DeploymentID: "deployment-a", ReleaseID: "release-a",
		Items: []QualificationRefreshItem{
			{Kind: DeploymentEvidenceProvider, ImportID: "import-provider"},
			{Kind: DeploymentEvidenceMigration, ImportID: "import-migration", SupersedesLinkID: "link-old"},
		},
		AsOf: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC), IdempotencyKey: "refresh-1",
		Actor: AuditActor{ID: "operator", WorkspaceID: "workspace-a", Kind: "human"},
	}
	left, right := base, base
	left.Items = append([]QualificationRefreshItem(nil), base.Items...)
	right.Items = append([]QualificationRefreshItem(nil), base.Items...)
	left.Items[0], left.Items[1] = left.Items[1], left.Items[0]
	if err := left.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := right.Normalize(); err != nil {
		t.Fatal(err)
	}
	if left.StableHash() != right.StableHash() {
		t.Fatalf("request hash changed with item order")
	}
	if left.Items[0].Kind != DeploymentEvidenceMigration {
		t.Fatalf("items were not sorted")
	}
}

func TestQualificationRefreshRequestRejectsUnsafeShapes(t *testing.T) {
	base := QualificationRefreshRequest{
		WorkspaceID: "workspace-a", DeploymentID: "deployment-a", ReleaseID: "release-a",
		Items: []QualificationRefreshItem{{Kind: DeploymentEvidenceProvider, ImportID: "import-provider"}},
		AsOf:  time.Now().UTC(), IdempotencyKey: "refresh-1",
		Actor: AuditActor{ID: "operator", WorkspaceID: "workspace-a"},
	}
	cases := []struct {
		name   string
		mutate func(*QualificationRefreshRequest)
		want   string
	}{
		{"missing as_of", func(r *QualificationRefreshRequest) { r.AsOf = time.Time{} }, "as_of"},
		{"cross workspace actor", func(r *QualificationRefreshRequest) { r.Actor.WorkspaceID = "workspace-b" }, "workspace"},
		{"unknown kind", func(r *QualificationRefreshRequest) { r.Items[0].Kind = "unknown" }, "unknown"},
		{"duplicate kind", func(r *QualificationRefreshRequest) { r.Items = append(r.Items, r.Items[0]) }, "repeats kind"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := base
			request.Items = append([]QualificationRefreshItem(nil), base.Items...)
			tc.mutate(&request)
			if err := request.Normalize(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Normalize() error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestQualificationRefreshReportHashIgnoresCreationTime(t *testing.T) {
	report := QualificationRefreshReport{
		ID: "refresh-1", WorkspaceID: "workspace-a", DeploymentID: "deployment-a", ReleaseID: "release-a",
		AsOf: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC), RequestHash: strings.Repeat("a", 64),
		BeforeGateHash: strings.Repeat("b", 64), AfterGateHash: strings.Repeat("c", 64), Outcome: QualificationOutcomePassed,
		Items: []QualificationRefreshItemResult{{Kind: DeploymentEvidenceProvider, ImportID: "import-1", LinkID: "link-1", SignedHash: strings.Repeat("d", 64), ObservationHash: strings.Repeat("e", 64), ReportHash: strings.Repeat("f", 64), ManifestHash: strings.Repeat("1", 64), SourceHash: strings.Repeat("2", 64), Outcome: QualificationOutcomePassed}},
	}
	left, right := report, report
	left.CreatedAt = time.Unix(1, 0).UTC()
	right.CreatedAt = time.Unix(2, 0).UTC()
	if err := left.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := right.Normalize(); err != nil {
		t.Fatal(err)
	}
	if left.ReportHash != right.ReportHash {
		t.Fatalf("report hash depends on creation time")
	}
}
