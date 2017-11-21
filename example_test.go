package dbx_test

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsm/dbx"
)

func ExampleNewIterator() {
	// Create tables, seed some test data
	db, err := setupTestDB()
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

	// Output:
	// &{ID:321 Title:Post 321 Comments:[]}
	// &{ID:642 Title:Post 642 Comments:[]}
	// &{ID:963 Title:Post 963 Comments:[]}
}

func ExampleNewBatchIterator() {
	// Create tables, seed some test data
	db, err := setupTestDB()
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

	// Output:
	// &{ID:321 Title:Post 321 Comments:[]}
	// &{ID:642 Title:Post 642 Comments:[{ID:769 PostID:642 Message:Comment 642/1}]}
	// &{ID:963 Title:Post 963 Comments:[{ID:1154 PostID:963 Message:Comment 963/1} {ID:1155 PostID:963 Message:Comment 963/2}]}
}

func ExampleNewIncrementalIterator() {
	// Create tables, seed some test data
	db, err := setupTestDB()
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

	// Init an iterator AND defer Close()
	lastID := int64(0)
	iter := dbx.NewIncrementalIterator(func() (*sql.Rows, error) {
		return db.Query(`SELECT id, title FROM posts WHERE id > ? ORDER BY id LIMIT 400`, lastID)
	}, scanPost, nil)
	defer iter.Close()

	// Iterate over records, update lastID
	n := 0
	for iter.Next() {
		post := iter.Record().(*Post)
		lastID = post.ID

		if n++; n%321 == 0 {
			fmt.Printf("%+v\n", post)
		}
	}

	// Check for iterator errors
	if err := iter.Err(); err != nil {
		panic(err)
	}

	// Output:
	// &{ID:321 Title:Post 321 Comments:[]}
	// &{ID:642 Title:Post 642 Comments:[]}
	// &{ID:963 Title:Post 963 Comments:[]}
}
