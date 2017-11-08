package dbiter

// Iterator is used to iterate over records
type Iterator interface {
	// Record returns record at the current cursor position
	Record() interface{}
	// Next advances the cursor to the next record
	Next() bool
	// Err returns an error if any
	Err() error
	// Close closes the underlying rows and
	// release
	Close() error
}

// RowScanner is a simplified abstraction of sql.Rows
type RowScanner interface {
	// Scan scans the row into dest
	Scan(dest ...interface{}) error
}

// ScanFunc scans a row into a record
type ScanFunc func(RowScanner) (interface{}, error)

// TransformFunc transforms a batch of records
type TransformFunc func(records []interface{}) error
