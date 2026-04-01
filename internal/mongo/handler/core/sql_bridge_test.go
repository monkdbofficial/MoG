package mongo

import (
	"os"
	"testing"
)

func TestSQLTypeForFieldVectorSimilarity(t *testing.T) {
	restore := setEnv("MOG_FLOAT_VECTOR_SIMILARITY", "cosine_similarity")
	defer restore()

	field := "embedding"
	v := []interface{}{0.1, 0.2, 0.3}
	got := sqlTypeForField(field, v)
	want := "FLOAT_VECTOR(3) WITH (similarity = 'cosine')"
	if got != want {
		t.Fatalf("unexpected sql type: got %q want %q", got, want)
	}
}

func TestSQLTypeForFieldVectorSimilarityInvalid(t *testing.T) {
	restore := setEnv("MOG_FLOAT_VECTOR_SIMILARITY", "invalid")
	defer restore()

	field := "embedding"
	v := []interface{}{0.1, 0.2}
	got := sqlTypeForField(field, v)
	want := "FLOAT_VECTOR(2)"
	if got != want {
		t.Fatalf("unexpected sql type for invalid similarity: got %q want %q", got, want)
	}
}

func setEnv(key, value string) func() {
	prev, had := os.LookupEnv(key)
	if err := os.Setenv(key, value); err != nil {
		panic(err)
	}
	return func() {
		if !had {
			os.Unsetenv(key)
		} else {
			os.Setenv(key, prev)
		}
	}
}
