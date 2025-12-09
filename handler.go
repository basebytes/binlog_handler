package binlog_handler

import (
	"errors"
	"fmt"

	"github.com/basebytes/binlog"
	"github.com/basebytes/interceptor"
	"github.com/sirupsen/logrus"
)

func NewGeneralHandler[V any](name string, out chan<- []V, logger *logrus.Logger, generator ContextGenerator[V], chains []*interceptor.Chain[V]) (h *GeneralHandler[V], err error) {
	h = &GeneralHandler[V]{
		name:      name,
		out:       out,
		logger:    logger,
		generator: generator,
	}
	err = h.init(chains)
	return
}

type GeneralHandler[V any] struct {
	name      string
	out       chan<- []V
	logger    *logrus.Logger
	chains    map[string]*interceptor.Chain[V]
	generator ContextGenerator[V]
}

func (h *GeneralHandler[V]) Name() string {
	return h.name
}

func (h *GeneralHandler[V]) init(chains []*interceptor.Chain[V]) (err error) {
	h.chains = make(map[string]*interceptor.Chain[V])
	for _, chain := range chains {
		key := h.key(chain.Schema(), chain.Table())
		if _, ok := h.chains[key]; ok {
			err = fmt.Errorf("duplicate chain key[%s]", key)
			break
		}
		h.chains[key] = chain
	}
	return
}

func (h *GeneralHandler[V]) Handle(event *binlog.Event) {
	if chain, ctx, ok := h.prepare(event); ok {
		chain.Do(ctx)
		if err := ctx.Error(); err != nil && !errors.Is(err, interceptor.AbortErr) {
			h.logger.Errorf("process event error:%s", err.Error())
		} else if err == nil && len(ctx.Results()) > 0 {
			h.out <- ctx.Results()
		}
	}
}

func (h *GeneralHandler[V]) prepare(event *binlog.Event) (chain *interceptor.Chain[V], ctx interceptor.Context[V], ok bool) {
	if chain, ok = h.chains[h.key(event.Schema(), event.Table())]; ok {
		ctx, ok = h.generator(event, h.logger)
	}
	return
}

func (h *GeneralHandler[V]) key(schema, table string) string {
	return fmt.Sprintf(keyFmt, schema, table)
}

const keyFmt = "%s:%s"
