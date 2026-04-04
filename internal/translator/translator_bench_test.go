package translator

import (
	"testing"

	"gopkg.in/mgo.v2/bson"
)

func BenchmarkIsSafePath(b *testing.B) {
	const path = "user.profile.address.city"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !isSafePath(path) {
			b.Fatal("expected safe path")
		}
	}
}

func BenchmarkDataAccessor(b *testing.B) {
	const path = "user.profile.address.city"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = dataAccessor(path)
	}
}

func BenchmarkTranslateFindWithOptions(b *testing.B) {
	tr := New()
	filter := bson.M{
		"name":     "alice",
		"age":      bson.M{"$gte": 21},
		"score":    bson.M{"$lt": 99},
		"active":   true,
		"tenantId": "t1",
	}
	sortSpec := bson.M{
		"age":   1,
		"score": -1,
		"name":  1,
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := tr.TranslateFindWithOptions("users", filter, sortSpec, 10, 25)
		if err != nil {
			b.Fatal(err)
		}
	}
}

