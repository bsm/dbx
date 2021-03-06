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

func main() {{ "ExampleNewIterator" | code }}
```

Like above, just batching and with the ability to resolve (1:n) associations:

```go
import (
  "fmt"
  "strings"

  "github.com/bsm/dbx"
)

func main() {{ "ExampleNewBatchIterator" | code }}
```
