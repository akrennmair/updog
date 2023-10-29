# README for updog

[![Go Reference](https://pkg.go.dev/badge/github.com/akrennmair/updog.svg)](https://pkg.go.dev/github.com/akrennmair/updog)
[![Go Report Card](https://goreportcard.com/badge/github.com/akrennmair/updog)](https://goreportcard.com/report/github.com/akrennmair/updog)

## What's updog?

Not much, what's up with you?!

updog is a columnar index to very quickly run count queries, optionally allowing to group the result by specific columns.
If you're familiar with SQL, its functionality is equivalent to running `SELECT x, y, z, COUNT(*) FROM ... WHERE ... GROUP BY x, y, z`
queries.

The data source to query on (the `FROM` part of the SQL query) is limited to a single index (which you can imagine as a table), while
the query expression (the `WHERE` clause of the SQL query) is currently limited to the operators `=`, `NOT`, `AND` and `OR`. At the moment,
updog does not provide a textual query language. Queries need to be constructed as `Query` objects instead.

See the [Go Reference](https://pkg.go.dev/github.com/akrennmair/updog) for further details and a full documentation of the API.

## License

This project is licensed under the MIT license - see the [LICENSE](LICENSE) file for details.
