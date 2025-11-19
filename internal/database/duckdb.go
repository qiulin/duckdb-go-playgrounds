package database

import (
	"database/sql"
	"path"

	"github.com/duckdb/duckdb-go/v2"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/qiulin/duckdb-go-playgrounds/internal/conf"
)

func NewDuckConn(config *conf.Config) (*duckdb.Connector, error) {
	dbFile := path.Join(config.DuckDB.DataDir, "duckdb.db")
	c, err := duckdb.NewConnector(dbFile, nil)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func NewDuckDB(conn *duckdb.Connector) *sql.DB {
	return sql.OpenDB(conn)
}
