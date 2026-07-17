package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/poisnoir/spine-go/client-go"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()

	// BUGFIX: see example/publisher/publisher.go — spined only accepts "common" today.
	node, _ := spine.CreateNode("common", "subscriber_sample", ctx, logger)

	sub1, _ := spine.NewSubscriber[uint32](node, "temperature")

	for {
		fmt.Println(sub1.Get())
	}
}
