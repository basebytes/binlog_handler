package binlog_handler

import (
	"github.com/basebytes/binlog"
	"github.com/basebytes/interceptor"
	"github.com/sirupsen/logrus"
)

type Context[V any] interface {
	interceptor.Context[V]
	DB() string
	Schema() string
	Table() string
	Action() string
	Rows() []map[string]any
	DecodeRows() error
	Trace(data any, err error)
}

type ContextGenerator[V any] func(*binlog.Event, *logrus.Logger) (interceptor.Context[V], bool)

type Handler interface {
	Name() string
	Handle(event *binlog.Event)
}
