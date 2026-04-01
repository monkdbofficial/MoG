package opmsg

import "testing"

func TestParseVectorLimit(t *testing.T) {
	if got, err := parseVectorLimit(int64(5)); err != nil || got != 5 {
		t.Fatalf("expected limit 5, got %d err %v", got, err)
	}
	if _, err := parseVectorLimit("bad"); err == nil {
		t.Fatalf("expected error for invalid limit")
	}
}

func TestBuildVectorSearchSQL(t *testing.T) {
	got := buildVectorSearchSQL("testcol", "embedding", "[0.1,0.2]", 4, 3, "cosine")
	want := `WITH q AS (SELECT [0.1,0.2]::float_vector(4) AS v)
SELECT d.*, VECTOR_SIMILARITY(d.embedding, (SELECT v FROM q), 'cosine') AS "__mog_vectorSearchScore", VECTOR_SIMILARITY(d.embedding, (SELECT v FROM q), 'cosine') AS "_score"
FROM doc.testcol d
WHERE KNN_MATCH(d.embedding, (SELECT v FROM q), 3, 'cosine')
ORDER BY "__mog_vectorSearchScore" DESC
LIMIT 3`
	if got != want {
		t.Fatalf("unexpected vector search SQL:\ngot:\n%s\nwant:\n%s", got, want)
	}
}
