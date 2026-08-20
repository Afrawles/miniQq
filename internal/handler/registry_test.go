package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Afrawles/miniQq/internal/queue"
)

type testPayLoad struct {
	Kind     string `json:"kind"`
	Greeting string `json:"greeting"`
}

func printPayload(kind, greeting string) error {
	fmt.Printf("%s : %s\n\n", kind, greeting)
	return nil
}

func processorFunc(_ context.Context, payload queue.Payload) error {
	var tp testPayLoad
	err := json.Unmarshal(payload, &tp)

	if err != nil {
		return err
	}

	return printPayload(tp.Kind, tp.Greeting)
}

func TestRegistryRegisterGet(t *testing.T) {
	registry := &Registry{}
	kind := "test"

	t.Run("register", func(t *testing.T) {
		registry.Register(kind, processorFunc)
	})

	t.Run("get", func(t *testing.T) {
		ctx := context.Background()
		payload, err := json.Marshal(testPayLoad{Kind: "test", Greeting: "hello, World!"})

		if err != nil {
			t.Fatal(err)
		}

		handler, ok := registry.Get(kind)
		if !ok {
			t.Errorf("failed to get %q", kind)
		}

		if err := handler(ctx, payload); err != nil {
			t.Fatal(err)
		}
	})

}
