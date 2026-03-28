package mongo

import "testing"

func TestGlobalUniqueIndexRegistry_SharedAcrossHandlers(t *testing.T) {
	physical := "monkdb__idx"
	defs := []uniqueIndexDef{{name: "uniq_name", fields: []string{"name"}}}

	h1 := &Handler{}
	h2 := &Handler{}

	// Clean slate.
	h1.clearUniqueIndexes(physical)

	h1.addUniqueIndexes(physical, defs)
	got := h2.listUniqueIndexes(physical)
	if len(got) != 1 || got[0].name != "uniq_name" || len(got[0].fields) != 1 || got[0].fields[0] != "name" {
		t.Fatalf("unexpected unique indexes: %#v", got)
	}

	h2.clearUniqueIndexes(physical)
	if got := h1.listUniqueIndexes(physical); len(got) != 0 {
		t.Fatalf("expected cleared registry, got: %#v", got)
	}
}
