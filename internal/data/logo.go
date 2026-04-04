package data

import _ "embed"

// Logo is the embedded ASCII logo used by the MoG CLI/server output.
//
//go:embed images/logo.txt
var Logo string
