package agg

import "gopkg.in/mgo.v2/bson"

// cloneDocsShallow returns a shallow copy of each document in docs.
//
// It is used by stages that must avoid mutating caller-provided documents.
func cloneDocsShallow(docs []bson.M) []bson.M {
	out := make([]bson.M, 0, len(docs))
	for _, d := range docs {
		nd := bson.M{}
		for k, v := range d {
			nd[k] = v
		}
		out = append(out, nd)
	}
	return out
}
