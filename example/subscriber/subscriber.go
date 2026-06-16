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

	ns, _ := spine.JointNamespace("example", ctx, logger)

	sub1, _ := spine.NewSubscriber[uint32](ns, "temperature")
	_, _ = spine.NewSubscriber[uint32](ns, "temperature")

	for {
		fmt.Println(sub1.Get())
	}
}
