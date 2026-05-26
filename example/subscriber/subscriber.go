package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/poisnoir/spine-go"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ns, _ := spine.JointNamespace("example", "meow", logger)

	sub1, _ := spine.NewSubscriber[uint32](ns, "temperature")
	_, _ = spine.NewSubscriber[uint32](ns, "temperature")

	for {
		fmt.Println(sub1.Get())
	}

	select {}
}
