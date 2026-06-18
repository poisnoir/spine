package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/poisnoir/spine-go"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()

	node, _ := spine.CreateNode("example", "subscriber_sample", ctx, logger)

	sub1, _ := spine.NewSubscriber[uint32](node, "temperature")

	for {
		fmt.Println(sub1.Get())
	}
}
