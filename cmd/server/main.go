package main

import (
	"context"
	"os"

	"github.com/go-viper/mapstructure/v2"
	"github.com/labstack/echo/v4"
	"github.com/lynx-go/lynx"
	"github.com/lynx-go/lynx/contrib/log/zap"
	"github.com/lynx-go/lynx/server/http"
	"github.com/lynx-go/x/log"
	"github.com/qiulin/duckdb-go-playgrounds/internal/api"
	"github.com/qiulin/duckdb-go-playgrounds/internal/conf"
	"github.com/qiulin/duckdb-go-playgrounds/internal/database"
	"github.com/qiulin/duckdb-go-playgrounds/internal/service"
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
		if err := app.Config().Unmarshal(config, func(config *mapstructure.DecoderConfig) {
			config.TagName = "json"
		}); err != nil {
			return err
		}

		duckConn, err := database.NewDuckConn(config)
		if err != nil {
			return err
		}
		db, err := database.NewDuckDB(duckConn)
		if err != nil {
			return err
		}
		app.OnStop(func(ctx context.Context) error {
			log.InfoContext(ctx, "closing database connection")
			return db.Close()
		})

		batchWriter, err := service.NewBatchWriter("heartbeats", duckConn)
		if err != nil {
			return err
		}
		app.OnStart(func(ctx context.Context) error {
			return batchWriter.Start(ctx)
		})
		app.OnStop(func(ctx context.Context) error {
			return batchWriter.Stop(ctx)
		})
		writerApi, err := api.NewWriterAPI(batchWriter, db)
		if err != nil {
			return err
		}
		queryApi := api.NewQueryAPI(db)

		router := echo.New()
		router.POST("/api/write", writerApi.Write)
		router.POST("/api/cleanup", writerApi.CleanUp)
		router.GET("/api/query/onlines", queryApi.QueryOnlines)
		router.GET("/api/rows", queryApi.Rows)

		hs := http.NewServer(router, http.WithLogger(logger), http.WithAddr(config.Server.Http.Addr))
		if err := app.Hook(hs); err != nil {
			return err
		}

		return nil
	})

	app.Run()
}
