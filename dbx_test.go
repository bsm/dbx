package dbx_test

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsm/dbx"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "dbx")
}

var _ = BeforeSuite(func() {
	var err error

	testDB, err = setupTestDB()
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	Expect(testDB.Close()).To(Succeed())
})

// --------------------------------------------------------------------

var testDB *TestDB

type TestDB struct {
	*sql.DB
	dir string
}

func (db *TestDB) Close() error {
	err := db.DB.Close()
	_ = os.RemoveAll(db.dir)
	return err
}

func setupTestDB() (*TestDB, error) {
	dir, err := ioutil.TempDir("", "dbx-test")
	if err != nil {
		return nil, err
	}

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

	return &TestDB{DB: db, dir: dir}, nil
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

func transformPosts(recs []interface{}) error {
	postMap := make(map[int64]*Post, len(recs))
	postIDs := make([]interface{}, 0, len(recs))

	for _, rec := range recs {
		post := rec.(*Post)
		postMap[post.ID] = post
		postIDs = append(postIDs, post.ID)
	}

	rows, err := testDB.Query(`
    SELECT id, post_id, message
    FROM comments
    WHERE post_id IN (?`+strings.Repeat(",?", len(postIDs)-1)+`)`,
		postIDs...)
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
}
