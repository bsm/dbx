package dbiter

import "database/sql"

type simple struct {
	rows *sql.Rows

	sfn ScanFunc
	rec interface{}
	err error
}

// New creates a new iterator around rows.
func New(rows *sql.Rows, scanFn ScanFunc) Iterator {
	return &simple{
		rows: rows,
		sfn:  scanFn,
	}
}

// Record implements Iterator
func (i *simple) Record() interface{} { return i.rec }

// Next implements Iterator
func (i *simple) Next() bool {
	if i.err != nil {
		return false
	}

	if !i.rows.Next() {
		return false
	}

	i.rec, i.err = i.sfn(i.rows)
	return i.err == nil
}

// Err implements Iterator
func (i *simple) Err() error {
	if i.err != nil {
		return i.err
	}
	if err := i.rows.Err(); err != nil {
		i.err = err
	}
	return i.err
}

// Close implements Iterator
func (i *simple) Close() error {
	return i.rows.Close()
}
