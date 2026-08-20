package miniqq

import "github.com/Afrawles/miniQq/internal/handler"

type Handler = handler.Handler
type Registry = handler.Registry

func NewRegistry() *Registry {
	return &Registry{}
}
