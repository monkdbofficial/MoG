package mongo

import "mog/internal/mongo/handler/sql"

func sqlTypeForValue(v interface{}) string { return sql.TypeForValue(v) }

func floatVectorLiteral(v interface{}) (string, int, bool) { return sql.FloatVectorLiteral(v) }

func isUndefinedColumn(err error) bool     { return sql.IsUndefinedColumn(err) }
func isUndefinedRelation(err error) bool   { return sql.IsUndefinedRelation(err) }
func isUndefinedSchema(err error) bool     { return sql.IsUndefinedSchema(err) }
func isDuplicateColumn(err error) bool     { return sql.IsDuplicateColumn(err) }
func isDuplicateObject(err error) bool     { return sql.IsDuplicateObject(err) }
func isUniqueViolation(err error) bool     { return sql.IsUniqueViolation(err) }
func isDuplicateColumnName(err error) bool { return sql.IsDuplicateColumnName(err) }
