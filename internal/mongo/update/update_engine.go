package update

import (
	"fmt"
	"strings"

	"gopkg.in/mgo.v2/bson"

	"mog/internal/mongo/pipeline"
)

// Warning describes a non-fatal compatibility issue encountered while applying
// an update document.
type Warning struct {
	Op      string
	Path    string
	Message string
}

// ApplyUpdate applies a MongoDB-style update document to an input document and returns the updated copy.
// It never mutates the input doc (copy-update strategy for safety).
func ApplyUpdate(doc bson.M, update bson.M) (bson.M, []Warning) {
	out := pipeline.DeepCloneDoc(doc)
	var warnings []Warning

	// Replacement document (no operators): treat as full replace but keep _id stable.
	hasOp := false
	for k := range update {
		if strings.HasPrefix(k, "$") {
			hasOp = true
			break
		}
	}
	if !hasOp {
		repl := pipeline.DeepCloneDoc(update)
		// Preserve existing _id if replacement doesn't set it (or tries to change it).
		if oldID, ok := out["_id"]; ok {
			if newID, has := repl["_id"]; !has || fmt.Sprint(newID) != fmt.Sprint(oldID) {
				repl["_id"] = oldID
			}
		}
		return repl, warnings
	}

	for op, raw := range update {
		if !strings.HasPrefix(op, "$") {
			continue
		}
		spec, ok := pipeline.CoerceBsonM(raw)
		if !ok {
			warnings = append(warnings, Warning{Op: op, Message: "operator spec must be an object"})
			continue
		}
		switch op {
		case "$set":
			warnings = append(warnings, applySet(out, spec)...)
		case "$unset":
			warnings = append(warnings, applyUnset(out, spec)...)
		case "$inc":
			warnings = append(warnings, applyInc(out, spec)...)
		case "$push":
			warnings = append(warnings, applyPush(out, spec)...)
		case "$addToSet":
			warnings = append(warnings, applyAddToSet(out, spec)...)
		default:
			warnings = append(warnings, Warning{Op: op, Message: "unsupported update operator"})
		}
	}
	return out, warnings
}
