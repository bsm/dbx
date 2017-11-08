package dbiter

import (
	"database/sql"
)

type batch struct {
	rows *sql.Rows

	sfn ScanFunc
	tfn TransformFunc
	lim int
	err error

	batch []interface{}
	cur   int
	done  bool
}

// NewBatch creates a new batch iterator around rows.
func NewBatch(rows *sql.Rows, batchSize int, scanFn ScanFunc, trfmFn TransformFunc) Iterator {
	return &batch{
		rows: rows,
		sfn:  scanFn,
		tfn:  trfmFn,
		cur:  -1,
		lim:  batchSize,
	}
}

// Record implements Iterator
func (i *batch) Record() interface{} {
	if i.cur > -1 && i.cur < len(i.batch) {
		return i.batch[i.cur]
	}
	return nil
}

// Next implements Iterator
func (i *batch) Next() bool {
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
func (i *batch) Err() error { return i.err }

// Close implements Iterator
func (i *batch) Close() error {
	return i.rows.Close()
}

func (i *batch) next() error {
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
	if i.tfn != nil {
		if err := i.tfn(i.batch); err != nil {
			return err
		}
	}
	return nil
}
