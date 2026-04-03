package mongo

import "testing"

func TestCatalogCache_ListCollections_FiltersInternal(t *testing.T) {
	c := &catalogCache{seen: map[string]struct{}{}}
	c.add("testdb", "users")
	c.add("testdb", "users__graph_edges__nvqw4ylhmvzf62le_ovzwk4s7nfsa")
	c.add("testdb", "users__graph_vertices__abc")

	got := c.listCollections("testdb")
	if len(got) != 1 || got[0] != "users" {
		t.Fatalf("unexpected collections: %#v", got)
	}
}

