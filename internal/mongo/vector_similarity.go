package mongo

import "strings"

// NormalizeVectorSimilarity returns a canonical similarity name accepted by MonkDB,
// or false if the provided value is not recognized.
func NormalizeVectorSimilarity(raw string) (string, bool) {
	value := strings.TrimSpace(strings.ToLower(raw))
	switch value {
	case "euclidean", "l2":
		return "euclidean", true
	case "cosine", "cosine_similarity", "cosine-similarity":
		return "cosine", true
	case "dot_product", "dotproduct", "dot-product":
		return "dot_product", true
	case "maximum_inner_product", "max_inner_product", "mips", "max-inner-product":
		return "maximum_inner_product", true
	default:
		return "", false
	}
}
