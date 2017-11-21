package dbx_test

import (
	"database/sql"

	"github.com/bsm/dbx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Iterator", func() {
	var (
		scanCount int
		scanFunc  = func(rs dbx.RowScanner) (interface{}, error) {
			scanCount++
			return scanPost(rs)
		}

		transformCount int
		transformFunc  = func(recs []interface{}) error {
			transformCount++
			return transformPosts(recs)
		}
	)

	BeforeEach(func() {
		scanCount = 0
		transformCount = 0
	})

	It("should iterate", func() {
		rows, err := testDB.Query(`SELECT id, title FROM posts`)
		Expect(err).NotTo(HaveOccurred())

		iter := dbx.NewIterator(rows, scanFunc)
		defer iter.Close()

		var posts []*Post
		for iter.Next() {
			posts = append(posts, iter.Record().(*Post))
		}
		Expect(iter.Err()).NotTo(HaveOccurred())
		Expect(iter.Close()).To(Succeed())

		Expect(posts).To(HaveLen(1234))
		Expect(posts[432]).To(Equal(&Post{
			ID: 433, Title: "Post 433",
		}))
		Expect(scanCount).To(Equal(1234))
	})

	It("should iterate over batches", func() {
		rows, err := testDB.Query(`SELECT id, title FROM posts`)
		Expect(err).NotTo(HaveOccurred())

		iter := dbx.NewBatchIterator(rows, 200, scanFunc, nil)
		defer iter.Close()

		var posts []*Post
		for iter.Next() {
			posts = append(posts, iter.Record().(*Post))
		}
		Expect(iter.Err()).NotTo(HaveOccurred())
		Expect(iter.Close()).To(Succeed())

		Expect(posts).To(HaveLen(1234))
		Expect(posts[432]).To(Equal(&Post{
			ID:    433,
			Title: "Post 433",
		}))
		Expect(scanCount).To(Equal(1234))
	})

	It("should iterate over batches with transform", func() {
		rows, err := testDB.Query(`SELECT id, title FROM posts`)
		Expect(err).NotTo(HaveOccurred())

		iter := dbx.NewBatchIterator(rows, 200, scanFunc, transformFunc)
		defer iter.Close()

		var posts []*Post
		for iter.Next() {
			posts = append(posts, iter.Record().(*Post))
		}
		Expect(iter.Err()).NotTo(HaveOccurred())
		Expect(iter.Close()).To(Succeed())

		Expect(posts).To(HaveLen(1234))
		Expect([]*Post{posts[13], posts[432], posts[1231]}).To(Equal([]*Post{
			{
				ID:    14,
				Title: "Post 14",
				Comments: []Comment{
					{ID: 16, PostID: 14, Message: "Comment 14/1"},
					{ID: 17, PostID: 14, Message: "Comment 14/2"},
					{ID: 18, PostID: 14, Message: "Comment 14/3"},
				},
			},
			{
				ID:    433,
				Title: "Post 433",
				Comments: []Comment{
					{ID: 518, PostID: 433, Message: "Comment 433/1"},
					{ID: 519, PostID: 433, Message: "Comment 433/2"},
				},
			},
			{
				ID:    1232,
				Title: "Post 1232",
				Comments: []Comment{
					{ID: 1477, PostID: 1232, Message: "Comment 1232/1"},
				},
			},
		}))
		Expect(scanCount).To(Equal(1234))
		Expect(transformCount).To(Equal(7))
	})

	It("should iterate incrementally", func() {
		lastID := int64(0)
		queryQueues := make([]int64, 0)

		iter := dbx.NewIncrementalIterator(func() (*sql.Rows, error) {
			queryQueues = append(queryQueues, lastID)

			return testDB.Query(`SELECT id, title FROM posts WHERE id > ? ORDER BY id LIMIT 300`, lastID)
		}, scanFunc, transformFunc)
		defer iter.Close()

		var posts []*Post
		for iter.Next() {
			post := iter.Record().(*Post)
			lastID = post.ID
			posts = append(posts, post)
		}
		Expect(iter.Err()).NotTo(HaveOccurred())
		Expect(iter.Close()).To(Succeed())

		Expect(posts).To(HaveLen(1234))
		Expect([]*Post{posts[13], posts[432], posts[1231]}).To(Equal([]*Post{
			{
				ID:    14,
				Title: "Post 14",
				Comments: []Comment{
					{ID: 16, PostID: 14, Message: "Comment 14/1"},
					{ID: 17, PostID: 14, Message: "Comment 14/2"},
					{ID: 18, PostID: 14, Message: "Comment 14/3"},
				},
			},
			{
				ID:    433,
				Title: "Post 433",
				Comments: []Comment{
					{ID: 518, PostID: 433, Message: "Comment 433/1"},
					{ID: 519, PostID: 433, Message: "Comment 433/2"},
				},
			},
			{
				ID:    1232,
				Title: "Post 1232",
				Comments: []Comment{
					{ID: 1477, PostID: 1232, Message: "Comment 1232/1"},
				},
			},
		}))
		Expect(scanCount).To(Equal(1234))
		Expect(queryQueues).To(Equal([]int64{0, 300, 600, 900, 1200, 1234}))
	})

})
