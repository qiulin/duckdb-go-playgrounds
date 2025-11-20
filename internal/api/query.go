package api

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/huandu/go-sqlbuilder"
	"github.com/labstack/echo/v4"
	"github.com/lynx-go/x/log"
)

type QueryAPI struct {
	db *sql.DB
}

func NewQueryAPI(db *sql.DB) *QueryAPI {
	return &QueryAPI{
		db: db,
	}
}

func (api *QueryAPI) Rows(ctx echo.Context) error {
	rows, err := api.db.Query("SELECT COUNT(*) FROM heartbeats")
	if err != nil {
		return err
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return err
		}
		break
	}
	return ctx.JSON(200, map[string]interface{}{
		"count": count,
	})
}

// QueryMinuteRetention 计算每分钟的留存
func (api *QueryAPI) QueryMinuteRetention(ctx echo.Context) error {
	req := &QueryRequest{}
	if err := ctx.Bind(req); err != nil {
		return err
	}

	return nil
}

func (api *QueryAPI) queryMinuteRetention(ctx context.Context, req *QueryRequest) error {

	return nil
}

const minuteRetentionQuery = `

`

func (api *QueryAPI) QueryOnlines(ctx echo.Context) error {
	req := &QueryRequest{}
	if err := ctx.Bind(req); err != nil {
		return err
	}
	results, err := api.queryOnlines(ctx.Request().Context(), req)
	if err != nil {
		log.ErrorContext(ctx.Request().Context(), "failed to query", err)
		return err
	}

	return ctx.JSON(http.StatusOK, results)
}

func (api *QueryAPI) queryOnlines(ctx context.Context, req *QueryRequest) (QueryResults, error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("room_id", "APPROX_COUNT_DISTINCT(user_id) AS user_onlines")
	sb.From("heartbeats")
	sb.Where("created_at >= now() - INTERVAL 1 MINUTE")
	if req.RoomType != nil {
		sb.Where(sb.EQ("room_type", req.RoomType))
	}
	if req.ServerID != nil {
		sb.Where(sb.EQ("server_id", req.ServerID))
	}
	sb.GroupBy("room_id")
	sb.OrderByDesc("user_onlines")
	sb.OrderByAsc("room_id")
	sb.Limit(200)
	query, args := sb.Build()
	log.InfoContext(ctx, "build query sql", "query", query, "args", args)
	rows, err := api.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make(QueryResults, 0)
	for rows.Next() {
		v := QueryResult{}
		if err := rows.Scan(&v.RoomID, &v.Onlines); err != nil {
			return nil, err
		}
		results = append(results, v)
	}
	return results, nil
}

type QueryResult struct {
	RoomID  int `json:"room_id"`
	Onlines int `json:"onlines"`
}

type QueryResults []QueryResult

type QueryRequest struct {
	ServerID *int `json:"server_id" form:"server_id" query:"server_id"`
	RoomType *int `json:"room_type" form:"room_type" query:"room_type"`
}
