package api

import (
	"database/sql"

	"github.com/labstack/echo/v4"
)

type QueryAPI struct {
	db *sql.DB
}

func NewQueryAPI(db *sql.DB) *QueryAPI {
	return &QueryAPI{
		db: db,
	}
}

func (api *QueryAPI) Query(ctx echo.Context) error {

	return nil
}
