package service

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/lynx-go/x/log"
	"github.com/wind-c/bqueue"
	"golang.org/x/sync/singleflight"
)

type BatchWriter struct {
	appender *duckdb.Appender
	batch    *bqueue.BatchQueue
	table    string
	single   *singleflight.Group
}

func NewBatchWriter(table string, conn *duckdb.Connector) (*BatchWriter, error) {
	c, err := conn.Connect(context.TODO())
	if err != nil {
		return nil, err
	}
	appender, err := duckdb.NewAppenderFromConn(c, "", table)
	if err != nil {
		return nil, err
	}
	single := &singleflight.Group{}
	writer := &BatchWriter{
		appender: appender,
		table:    table,
		single:   single,
	}
	batch := bqueue.NewBatchQueue(&bqueue.Options{
		Interval:      1 * time.Second,
		MaxBatchItems: 100,
		MaxQueueSize:  1024,
	})
	writer.batch = batch
	return writer, nil
}

func (bw *BatchWriter) Append(args ...driver.Value) error {
	bw.batch.Enqueue(args)
	return nil
}

func (bw *BatchWriter) runLoop(ctx context.Context) {
	for {
		select {
		case batch := <-bw.batch.OutQueue:
			for _, item := range batch {
				args := item.([]driver.Value)
				if err := bw.appender.AppendRow(args...); err != nil {
					log.ErrorContext(ctx, "append row error", err)
				}
			}
			if len(batch) > 0 {
				_, err, _ := bw.single.Do(bw.table, func() (interface{}, error) {
					log.InfoContext(ctx, "flush batch", "rows", len(batch))
					if err := bw.appender.Flush(); err != nil {
						log.ErrorContext(ctx, "appender flush error", err)
					}
					return nil, nil
				})
				if err != nil {
					log.ErrorContext(ctx, "singleflight flush batch error", err)
				}
			}
		}
	}
}

func (bw *BatchWriter) Start(ctx context.Context) error {
	log.InfoContext(ctx, "starting batch writer", "table", bw.table)
	go bw.batch.Start()
	go bw.runLoop(ctx)
	return nil
}

func (bw *BatchWriter) Stop(ctx context.Context) error {
	log.InfoContext(ctx, "stopping batch writer", "table", bw.table)
	bw.batch.Stop()
	return bw.appender.Close()
}
