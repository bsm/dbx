package dbx_test

import (
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
		Expect(posts[432]).To(Equal(&Post{
			ID:    433,
			Title: "Post 433",
			Comments: []Comment{
				{ID: 518, PostID: 433, Message: "Comment 433/1"},
				{ID: 519, PostID: 433, Message: "Comment 433/2"},
			},
		}))
		Expect(scanCount).To(Equal(1234))
		Expect(transformCount).To(Equal(7))
	})

})
