package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path"

	"github.com/duckdb/duckdb-go/v2"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lynx-go/x/log"
	"github.com/qiulin/duckdb-go-playgrounds/internal/conf"
)

func NewDuckConn(config *conf.Config) (*duckdb.Connector, error) {
	dbFile := path.Join(config.DuckDB.DataDir, "duckdb.db")
	log.InfoContext(context.TODO(), "Opening DuckDB DB file", "db_file", dbFile)
	c, err := duckdb.NewConnector(dbFile, func(execer driver.ExecerContext) error {
		return migrateSchema(execer)
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

func NewDuckDB(conn *duckdb.Connector) (*sql.DB, error) {
	db := sql.OpenDB(conn)
	return db, nil
}

func migrateSchema(db driver.ExecerContext) error {
	log.InfoContext(context.TODO(), "Migrating DuckDB schema")
	_, err := db.ExecContext(context.TODO(), "CREATE TABLE IF NOT EXISTS heartbeats (id VARCHAR, user_id INTEGER, room_id INTEGER, server_id INTEGER, room_type INTEGER, created_at TIMESTAMPTZ)", []driver.NamedValue{})
	if err != nil {
		return err
	}
	return nil
}
