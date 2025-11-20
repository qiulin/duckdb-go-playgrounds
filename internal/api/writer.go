package api

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/lynx-go/x/log"
	"github.com/qiulin/duckdb-go-playgrounds/internal/service"
)

type WriterAPI struct {
	writer *service.BatchWriter
	db     *sql.DB
}

func NewWriterAPI(
	writer *service.BatchWriter,
	db *sql.DB,
) (*WriterAPI, error) {
	return &WriterAPI{
		writer: writer,
		db:     db,
	}, nil
}

func (api *WriterAPI) Write(ctx echo.Context) error {
	req := &WriteRequest{}
	if err := ctx.Bind(req); err != nil {
		return err
	}
	if err := api.writer.Append(uuid.NewString(), req.UserID, req.RoomID, req.ServerID, req.RoomType, time.UnixMilli(int64(req.CreatedAt))); err != nil {
		return err
	}

	return ctx.JSON(200, map[string]interface{}{
		"error": map[string]any{
			"code":    0,
			"message": "ok",
		},
	})
}

func (api *WriterAPI) CleanUp(ctx echo.Context) error {
	if _, err := api.db.Exec("DELETE FROM heartbeats WHERE created_at < now() - INTERVAL 10 MINUTE"); err != nil {
		log.ErrorContext(ctx.Request().Context(), "delete expired records error", err)
		return err
	}
	if _, err := api.db.Exec("CHECKPOINT"); err != nil {
		log.ErrorContext(ctx.Request().Context(), "checkpoint error", err)
		return err
	}
	return ctx.JSON(200, map[string]interface{}{})
}

type WriteRequest struct {
	UserID    int `json:"user_id"`
	RoomID    int `json:"room_id"`
	RoomType  int `json:"room_type"`
	ServerID  int `json:"server_id"`
	CreatedAt int `json:"created_at"`
}
