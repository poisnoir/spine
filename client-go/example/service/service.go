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

	node, err := spine.CreateNode("common", "subscriber_sample", ctx, logger)
	if err != nil {
		panic(err)
	}

	lenFunc := func(i uint32) (uint32, error) {
		return i * 2, nil
	}

	_, err = spine.NewService(node, "time_two", lenFunc)
	if err != nil {
		panic(err)
	}

	fmt.Println("service started")

	select {}

}
