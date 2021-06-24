package dbx_test

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/bsm/dbx"
)

func TestIterator(t *testing.T) {
	db, err := setupTestDB(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT id, title FROM posts`)
	if err != nil {
		t.Fatal(err)
	}

	var scanCount int
	iter := dbx.NewIterator(rows, func(rs dbx.RowScanner) (interface{}, error) {
		scanCount++
		return scanPost(rs)
	})
	defer iter.Close()

	posts := drainPosts(t, iter, nil)
	if exp, got := 1234, scanCount; exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := 1234, len(posts); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := (&Post{ID: 433, Title: "Post 433"}), posts[432]; !reflect.DeepEqual(exp, got) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestIterator_batch(t *testing.T) {
	db, err := setupTestDB(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT id, title FROM posts`)
	if err != nil {
		t.Fatal(err)
	}

	var scanCount int
	iter := dbx.NewBatchIterator(rows, 200, func(rs dbx.RowScanner) (interface{}, error) {
		scanCount++
		return scanPost(rs)
	}, nil)
	defer iter.Close()

	posts := drainPosts(t, iter, nil)
	if exp, got := 1234, scanCount; exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := 1234, len(posts); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := (&Post{ID: 433, Title: "Post 433"}), posts[432]; !reflect.DeepEqual(exp, got) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestIterator_batchWithTransform(t *testing.T) {
	db, err := setupTestDB(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT id, title FROM posts`)
	if err != nil {
		t.Fatal(err)
	}

	var scanCount, transformCount int
	iter := dbx.NewBatchIterator(rows, 200, func(rs dbx.RowScanner) (interface{}, error) {
		scanCount++
		return scanPost(rs)
	}, func(recs []interface{}) error {
		transformCount++

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
			val, err := scanComment(rows)
			if err != nil {
				return err
			}
			comment := val.(*Comment)
			post := postMap[comment.PostID]
			post.Comments = append(post.Comments, *comment)
		}
		return rows.Err()
	})
	defer iter.Close()

	posts := drainPosts(t, iter, nil)
	if exp, got := 1234, scanCount; exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := 7, transformCount; exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := 1234, len(posts); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := (&Post{
		ID: 14, Title: "Post 14",
		Comments: []Comment{
			{ID: 16, PostID: 14, Message: "Comment 14/1"},
			{ID: 17, PostID: 14, Message: "Comment 14/2"},
			{ID: 18, PostID: 14, Message: "Comment 14/3"},
		},
	}), posts[13]; !reflect.DeepEqual(exp, got) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := (&Post{
		ID: 433, Title: "Post 433",
		Comments: []Comment{
			{ID: 518, PostID: 433, Message: "Comment 433/1"},
			{ID: 519, PostID: 433, Message: "Comment 433/2"},
		},
	}), posts[432]; !reflect.DeepEqual(exp, got) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := (&Post{
		ID: 1232, Title: "Post 1232",
		Comments: []Comment{
			{ID: 1477, PostID: 1232, Message: "Comment 1232/1"},
		},
	}), posts[1231]; !reflect.DeepEqual(exp, got) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestIterator_incremental(t *testing.T) {
	db, err := setupTestDB(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var lastID int64
	var markers []int64
	var scanCount int

	iter := dbx.NewIncrementalIterator(func() (*sql.Rows, error) {
		markers = append(markers, lastID)

		return db.Query(`SELECT id, title FROM posts WHERE id > ? ORDER BY id LIMIT 300`, lastID)
	}, func(rs dbx.RowScanner) (interface{}, error) {
		scanCount++
		return scanPost(rs)
	}, nil)
	defer iter.Close()

	posts := drainPosts(t, iter, func(post *Post) { lastID = post.ID })
	if exp, got := 1234, scanCount; exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := []int64{0, 300, 600, 900, 1200, 1234}, markers; !reflect.DeepEqual(exp, got) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := 1234, len(posts); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
	if exp, got := (&Post{ID: 433, Title: "Post 433"}), posts[432]; !reflect.DeepEqual(exp, got) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}
