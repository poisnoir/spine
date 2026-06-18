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

	lenFunc := func(input string) (uint32, error) {
		return uint32(len(input)), nil
	}

	printFunc := func(input string) (string, error) {
		fmt.Println(input)
		return "printed " + input, nil
	}

	_, _ = spine.NewService(node, "string_length", lenFunc)
	_, _ = spine.NewService(node, "print", printFunc)

	select {}

}
