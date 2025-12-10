package binlog_handler

import (
	"slices"

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

type Updates []string

func (u Updates) IsUpdated(column string) bool {
	return slices.Contains[[]string, string](u, column)
}

func (u Updates) Ignore(expected []string) bool {
	for _, exp := range expected {
		if slices.Contains[[]string, string](u, exp) {
			return false
		}
	}
	return true
}
