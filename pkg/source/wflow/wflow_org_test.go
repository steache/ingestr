package wflow

import (
	"reflect"
	"testing"
)

// The organization must lead every merge key. If it does not, two companies
// sharing an id collapse into one row in the pooled destination table.
func TestPrimaryKeysAreOrgScoped(t *testing.T) {
	cases := map[string][]string{
		"documents":          {"organization", "id"},
		"registers_partners": {"organization", "id"},
		"document_approvals": {"organization", "documentId", "level"},
	}
	for table, want := range cases {
		if got := primaryKeysFor(table); !reflect.DeepEqual(got, want) {
			t.Errorf("primaryKeysFor(%q) = %v, want %v", table, got, want)
		}
	}
}

// primaryKeysFor must not mutate the shared tablePrimaryKeys map -- an append
// onto the stored slice would grow it on every call.
func TestPrimaryKeysForDoesNotMutateTheMap(t *testing.T) {
	before := append([]string(nil), tablePrimaryKeys["document_approvals"]...)
	for i := 0; i < 3; i++ {
		primaryKeysFor("document_approvals")
	}
	if got := tablePrimaryKeys["document_approvals"]; !reflect.DeepEqual(got, before) {
		t.Errorf("tablePrimaryKeys mutated: %v, want %v", got, before)
	}
}
