package dbx

import "database/sql"

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

// --------------------------------------------------------------------

// ScanFunc scans a row into a record
type ScanFunc func(RowScanner) (interface{}, error)

type simpleIter struct {
	rows *sql.Rows

	sfn ScanFunc
	rec interface{}
	err error
}

// NewIterator creates a new iterator around rows.
func NewIterator(rows *sql.Rows, scan ScanFunc) Iterator {
	return &simpleIter{
		rows: rows,
		sfn:  scan,
	}
}

// Record implements Iterator
func (i *simpleIter) Record() interface{} { return i.rec }

// Next implements Iterator
func (i *simpleIter) Next() bool {
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
func (i *simpleIter) Err() error {
	if i.err != nil {
		return i.err
	}
	if err := i.rows.Err(); err != nil {
		i.err = err
	}
	return i.err
}

// Close implements Iterator
func (i *simpleIter) Close() error {
	return i.rows.Close()
}

// --------------------------------------------------------------------

// TransformFunc transforms a batch of records
type TransformFunc func(records []interface{}) error

type batchIter struct {
	rows *sql.Rows

	sfn ScanFunc
	tfn TransformFunc
	lim int
	err error

	batch []interface{}
	cur   int
	done  bool
}

// NewBatchIterator creates a new batch iterator around rows.
func NewBatchIterator(rows *sql.Rows, batchSize int, scan ScanFunc, transform TransformFunc) Iterator {
	return &batchIter{
		rows: rows,
		sfn:  scan,
		tfn:  transform,
		cur:  -1,
		lim:  batchSize,
	}
}

// Record implements Iterator
func (i *batchIter) Record() interface{} {
	if i.cur > -1 && i.cur < len(i.batch) {
		return i.batch[i.cur]
	}
	return nil
}

// Next implements Iterator
func (i *batchIter) Next() bool {
	if i.err != nil || i.done {
		return false
	}

	// Try to advance to the next batched record
	if i.cur+1 < len(i.batch) {
		i.cur++
		return true
	}

	// Fetch next batch
	if err := i.next(); err != nil {
		i.err = err
		return false
	}

	// Try to advance again
	if i.cur+1 < len(i.batch) {
		i.cur++
		return true
	}

	// Give up
	i.done = true
	return false
}

// Err implements Iterator
func (i *batchIter) Err() error { return i.err }

// Close implements Iterator
func (i *batchIter) Close() error {
	return i.rows.Close()
}

func (i *batchIter) next() error {
	i.batch = i.batch[:0]
	i.cur = -1

	for i.rows.Next() {
		rec, err := i.sfn(i.rows)
		if err != nil {
			return err
		}
		if i.batch = append(i.batch, rec); len(i.batch) >= i.lim {
			break
		}
	}
	if err := i.rows.Err(); err != nil {
		return err
	}
	if i.tfn != nil && len(i.batch) != 0 {
		if err := i.tfn(i.batch); err != nil {
			return err
		}
	}
	return nil
}
