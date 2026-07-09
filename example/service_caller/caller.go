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
	node, err := spine.CreateNode("common", "service_caller_sample", ctx, logger)
	if err != nil {
		panic(err)
	}

	c, err := spine.NewServiceCaller[uint32, uint32](node, "time_two")
	if err != nil {
		panic(err)
	}

	result, _ := c.Call(2, ctx)
	fmt.Println(result)

	select {}
}
