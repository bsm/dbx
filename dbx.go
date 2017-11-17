// Package dbx provides useful extensions to stdlib's database/sql
package dbx

// RowScanner is a simplified abstraction of sql.Rows
type RowScanner interface {
	// Scan scans the row into dest
	Scan(dest ...interface{}) error
}
