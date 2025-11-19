package main

import (
	"context"
	"os"

	"github.com/labstack/echo/v4"
	"github.com/lynx-go/lynx"
	"github.com/lynx-go/lynx/contrib/log/zap"
	"github.com/lynx-go/lynx/server/http"
	"github.com/qiulin/duckdb-go-playgrounds/internal/api"
	"github.com/qiulin/duckdb-go-playgrounds/internal/conf"
	"github.com/qiulin/duckdb-go-playgrounds/internal/database"
	"github.com/samber/lo"
)

func main() {
	opts := lynx.NewOptions(
		lynx.WithID(lo.Must1(os.Hostname())),
		lynx.WithName("duckdb-go-playgrounds"),
		lynx.WithUseDefaultConfigFlagsFunc(),
	)

	app := lynx.New(opts, func(ctx context.Context, app lynx.Lynx) error {
		logger := zap.NewLogger(app)
		app.SetLogger(logger)
		config := &conf.Config{}
		if err := app.Config().Unmarshal(config); err != nil {
			return err
		}

		duckConn, err := database.NewDuckConn(config)
		if err != nil {
			return err
		}
		app.OnStop(func(ctx context.Context) error {
			return duckConn.Close()
		})
		db := database.NewDuckDB(duckConn)
		app.OnStop(func(ctx context.Context) error {
			return db.Close()
		})
		app.OnStart(func(ctx context.Context) error {
			_, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS heartbeats (id VARCHAR, user_id INTEGER, room_id INTEGER, server_id INTEGER, room_type INTEGER, created_at TIMESTAMP)")
			return err
		})

		writerApi, err := api.NewWriterAPI(duckConn)
		if err != nil {
			return err
		}
		queryApi := api.NewQueryAPI(db)

		router := echo.New()
		router.POST("/api/write", writerApi.Write)
		router.GET("/api/query", queryApi.Query)

		hs := http.NewServer(router, http.WithLogger(logger), http.WithAddr(config.Server.Http.Addr))
		if err := app.Hook(hs); err != nil {
			return err
		}

		return nil
	})

	app.Run()
}
