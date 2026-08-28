package adyen

import "github.com/bruin-data/ingestr/internal/registry"

func init() {
	registry.RegisterSource(
		[]string{"adyen"},
		func() interface{} { return NewAdyenSource() },
	)
}
