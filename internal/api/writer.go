package api

import (
	"context"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

type WriterAPI struct {
	appender *duckdb.Appender
}

func NewWriterAPI(conn *duckdb.Connector) (*WriterAPI, error) {
	c, err := conn.Connect(context.TODO())
	if err != nil {
		return nil, err
	}
	appender, err := duckdb.NewAppenderFromConn(c, "", "heartbeats")
	if err != nil {
		return nil, err
	}
	return &WriterAPI{
		appender: appender,
	}, nil
}

func (api *WriterAPI) Write(ctx echo.Context) error {
	req := &WriteRequest{}
	if err := ctx.Bind(req); err != nil {
		return err
	}
	if err := api.appender.AppendRow(uuid.NewString(), req.UserID, req.RoomID, req.ServerID, req.RoomType, time.UnixMilli(int64(req.Timestamp))); err != nil {
		return err
	}

	return nil
}

type WriteRequest struct {
	UserID    int `json:"user_id"`
	RoomID    int `json:"room_id"`
	RoomType  int `json:"room_type"`
	ServerID  int `json:"server_id"`
	Timestamp int `json:"timestamp"`
}
