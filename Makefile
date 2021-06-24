default: test

test:
	go test ./...

staticcheck:
	staticcheck ./...

README.md: README.md.tpl $(wildcard *.go)
	becca -package github.com/bsm/dbx
