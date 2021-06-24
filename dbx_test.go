package dbx_test

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bsm/dbx"
	_ "github.com/mattn/go-sqlite3"
)

func setupTestDB(dir string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", filepath.Join(dir, "test.sqlite3"))
	if err != nil {
		return nil, err
	}

	if _, err := db.Exec(`
    CREATE TABLE posts (
      id INTEGER PRIMARY KEY NOT NULL,
      title VARCHAR(255) NOT NULL
    )
  `); err != nil {
		_ = db.Close()
		return nil, err
	}

	if _, err := db.Exec(`
    CREATE TABLE comments (
      id INTEGER PRIMARY KEY NOT NULL,
      post_id INTEGER NOT NULL,
      message VARCHAR(255) NOT NULL,
      FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
    )
  `); err != nil {
		_ = db.Close()
		return nil, err
	}

	tx, err := db.Begin()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	defer tx.Rollback()

	for i := 1; i <= 1234; i++ {
		if _, err := tx.Exec(`INSERT INTO posts (id, title) VALUES (?, ?)`, i, fmt.Sprintf("Post %d", i)); err != nil {
			_ = db.Close()
			return nil, err
		}

		for j := 1; j < i%5; j++ {
			if _, err := tx.Exec(`INSERT INTO comments (post_id, message) VALUES (?, ?)`, i, fmt.Sprintf("Comment %d/%d", i, j)); err != nil {
				_ = db.Close()
				return nil, err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

// --------------------------------------------------------------------

type Post struct {
	ID       int64
	Title    string
	Comments []Comment
}

func scanPost(rs dbx.RowScanner) (interface{}, error) {
	post := new(Post)
	if err := rs.Scan(&post.ID, &post.Title); err != nil {
		return nil, err
	}
	return post, nil
}

func drainPosts(t *testing.T, iter dbx.Iterator, each func(*Post)) (posts []*Post) {
	t.Helper()

	for iter.Next() {
		post := iter.Record().(*Post)
		if each != nil {
			each(post)
		}
		posts = append(posts, post)
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	return posts
}

type Comment struct {
	ID      int64
	PostID  int64
	Message string
}

func scanComment(rs dbx.RowScanner) (interface{}, error) {
	comment := new(Comment)
	if err := rs.Scan(&comment.ID, &comment.PostID, &comment.Message); err != nil {
		return nil, err
	}
	return comment, nil
}
