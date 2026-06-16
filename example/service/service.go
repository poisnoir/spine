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

	lenFunc := func(input string) (uint32, error) {
		return uint32(len(input)), nil
	}

	printFunc := func(input string) (string, error) {
		fmt.Println(input)
		return "printed " + input, nil
	}

	_, _ = spine.NewService(ns, "string_length", lenFunc)
	_, _ = spine.NewService(ns, "print", printFunc)

	select {}

}
