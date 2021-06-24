# DBx

[![Test](https://github.com/bsm/dbx/actions/workflows/test.yml/badge.svg)](https://github.com/bsm/dbx/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/bsm/dbx?status.png)](http://godoc.org/github.com/bsm/dbx)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Useful extensions to stdlib's [database/sql](https://golang.org/pkg/database/sql).

## Iterators

A simple wrapper for [sql.Rows](https://golang.org/pkg/database/sql/#Rows) to iterate
over structs:

```go
import (
  "fmt"

  "github.com/bsm/dbx"
)

func main() {
	// Init a temp dir
	dir, err := os.MkdirTemp("", "dbx-example")
	if err != nil {
		panic(err)
	}

	// Create tables, seed some test data
	db, err := setupTestDB(dir)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Query rows
	rows, err := db.Query(`SELECT id, title FROM posts ORDER BY id`)
	if err != nil {
		panic(err)
	}

	// Wrap rows in an iterator AND defer Close()
	iter := dbx.NewIterator(rows, func(rs dbx.RowScanner) (interface{}, error) {
		post := new(Post)
		if err := rs.Scan(&post.ID, &post.Title); err != nil {
			return nil, err
		}
		return post, nil
	})
	defer iter.Close()

	// Iterate over records, print a few
	n := 0
	for iter.Next() {
		post := iter.Record().(*Post)
		if n++; n%321 == 0 {
			fmt.Printf("%+v\n", post)
		}
	}

	// Check for iterator errors
	if err := iter.Err(); err != nil {
		panic(err)
	}

}
```

Like above, just batching and with the ability to resolve (1:n) associations:

```go
import (
  "fmt"
  "strings"

  "github.com/bsm/dbx"
)

func main() {
	// Init a temp dir
	dir, err := os.MkdirTemp("", "dbx-example")
	if err != nil {
		panic(err)
	}

	// Create tables, seed some test data
	db, err := setupTestDB(dir)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Scan each Post row into a struct (callback)
	scanPost := func(rs dbx.RowScanner) (interface{}, error) {
		post := new(Post)
		if err := rs.Scan(&post.ID, &post.Title); err != nil {
			return nil, err
		}
		return post, nil
	}

	// Scan each Comment row into a struct (callback)
	scanComment := func(rs dbx.RowScanner) (interface{}, error) {
		comment := new(Comment)
		if err := rs.Scan(&comment.ID, &comment.PostID, &comment.Message); err != nil {
			return nil, err
		}
		return comment, nil
	}

	// For every batch of Posts, fetch all the associated comments (callback)
	transformBatch := func(recs []interface{}) error {
		postMap := make(map[int64]*Post, len(recs))
		postIDs := make([]interface{}, 0, len(recs))

		for _, rec := range recs {
			post := rec.(*Post)
			postMap[post.ID] = post
			postIDs = append(postIDs, post.ID)
		}

		rows, err := db.Query(`SELECT id, post_id, message FROM comments WHERE post_id IN (?`+strings.Repeat(",?", len(postIDs)-1)+`)`, postIDs...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			v, err := scanComment(rows)
			if err != nil {
				return err
			}
			comment := v.(*Comment)

			post := postMap[comment.PostID]
			post.Comments = append(post.Comments, *comment)
		}
		return rows.Err()
	}

	// Query rows
	rows, err := db.Query(`SELECT id, title FROM posts ORDER BY id`)
	if err != nil {
		panic(err)
	}

	// Wrap rows in an iterator AND defer Close()
	iter := dbx.NewBatchIterator(rows, 100, scanPost, transformBatch)
	defer iter.Close()

	// Iterate over records, print a few
	n := 0
	for iter.Next() {
		post := iter.Record().(*Post)
		if n++; n%321 == 0 {
			fmt.Printf("%+v\n", post)
		}
	}

	// Check for iterator errors
	if err := iter.Err(); err != nil {
		panic(err)
	}

}
```
