package mongo

import "testing"

// TestNormalizeVectorSimilarity runs the corresponding test case.
func TestNormalizeVectorSimilarity(t *testing.T) {
	tests := map[string]string{
		"euclidean":             "euclidean",
		"L2":                    "euclidean",
		"cosine":                "cosine",
		"coSine_similarity":     "cosine",
		"dot_product":           "dot_product",
		"dotproduct":            "dot_product",
		"dot-product":           "dot_product",
		"maximum_inner_product": "maximum_inner_product",
		"max_inner_product":     "maximum_inner_product",
		"mips":                  "maximum_inner_product",
		"max-inner-product":     "maximum_inner_product",
	}
	for input, want := range tests {
		got, ok := NormalizeVectorSimilarity(input)
		if !ok {
			t.Fatalf("expected %q -> %s", input, want)
		}
		if got != want {
			t.Fatalf("normalize %q = %q want %q", input, got, want)
		}
	}
	if _, ok := NormalizeVectorSimilarity("invalid"); ok {
		t.Fatalf("expected invalid value to be rejected")
	}
}
