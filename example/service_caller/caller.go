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
	node, _ := spine.CreateNode("example", "service_caller_sample", ctx, logger)

	c, _ := spine.NewServiceCaller[string, string](node, "print")

	result, _ := c.Call("hello world", ctx)
	fmt.Println(result)

	select {}
}
