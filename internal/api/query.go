package api

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
	"text/template"
	"time"

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
	results, err := api.queryMinuteRetention(ctx.Request().Context(), req)
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusOK, results)
}

type RetentionResult struct {
	RoomID         int       `json:"room_id"`
	Minutes        time.Time `json:"-"`
	MinuteStr      string    `json:"minute"`
	TotalUsers     int       `json:"total_users"`
	RetainedUsers  int       `json:"retained_users"`
	RetentionRatio float64   `json:"retention_ratio"`
}

func truncMinute(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
}

func (api *QueryAPI) queryMinuteRetention(ctx context.Context, req *QueryRequest) ([]RetentionResult, error) {
	sb := &strings.Builder{}
	args := []any{}
	now := time.Now()
	args = append(args, truncMinute(now.AddDate(0, 0, -3)))
	args = append(args, truncMinute(now))
	if req.ServerID != nil {
		args = append(args, req.ServerID)
	}
	if req.RoomType != nil {
		args = append(args, req.RoomType)
	}
	err := minuteRetentionQueryTpl.Execute(sb, req)
	if err != nil {
		return nil, err
	}
	lastMinute := truncMinute(now.Add(-1 * time.Minute))
	args = append(args, lastMinute)
	query := sb.String()
	results := []RetentionResult{}
	rows, err := api.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var result RetentionResult
		if err := rows.Scan(&result.RoomID, &result.Minutes, &result.TotalUsers, &result.RetainedUsers, &result.RetentionRatio); err != nil {
			return nil, err
		}
		result.MinuteStr = result.Minutes.Format("2006-01-02 15:04:05")
		results = append(results, result)
	}

	return results, nil
}

var minuteRetentionQueryTpl = template.Must(template.New("minuteRetentionQuery").Parse(minuteRetentionQuery))

const minuteRetentionQuery = `
with
    alive_users as (
        select
            user_id,
            room_id,
            date_trunc ('minute', created_at) as minutes
        from
            heartbeats
        where
            created_at between ? and  ?
            {{ if .ServerID }} and server_id = ? {{ end }}
            {{ if .RoomType }} and room_type = ? {{ end }}
        group by
            user_id,
            room_id,
            date_trunc ('minute', created_at)
    ),
 minute_retention as (
	select
		a.room_id,
		a.minutes,
		APPROX_COUNT_DISTINCT (a.user_id) as total_users,
		APPROX_COUNT_DISTINCT (b.user_id) as retained_users,
		round(
			APPROX_COUNT_DISTINCT (b.user_id) / APPROX_COUNT_DISTINCT (a.user_id) * 100,
			1
		) as retention_ratio
	from
		alive_users a
		left join alive_users b on a.user_id = b.user_id
		and a.room_id = b.room_id
		and b.minutes = date_add (a.minutes, interval 1 minute)
	group by
		a.minutes,
		a.room_id
)
select * from minute_retention a
where a.minutes=?
order by
    a.retention_ratio desc,
    a.retained_users desc,
    a.room_id
limit 500
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

func (api *QueryAPI) queryOnlines(ctx context.Context, req *QueryRequest) (OnlineResults, error) {
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
	sb.Limit(500)
	query, args := sb.Build()
	log.InfoContext(ctx, "build query sql", "query", query, "args", args)
	rows, err := api.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make(OnlineResults, 0)
	for rows.Next() {
		v := OnlineResult{}
		if err := rows.Scan(&v.RoomID, &v.Onlines); err != nil {
			return nil, err
		}
		results = append(results, v)
	}
	return results, nil
}

type OnlineResult struct {
	RoomID  int `json:"room_id"`
	Onlines int `json:"onlines"`
}

type OnlineResults []OnlineResult

type QueryRequest struct {
	ServerID *int `json:"server_id" form:"server_id" query:"server_id"`
	RoomType *int `json:"room_type" form:"room_type" query:"room_type"`
}
