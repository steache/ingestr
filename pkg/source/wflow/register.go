package wflow

import "github.com/bruin-data/ingestr/internal/registry"

func init() {
	registry.RegisterSource(
		[]string{"wflow"},
		func() interface{} { return NewWflowSource() },
	)
}
