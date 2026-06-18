package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/poisnoir/spine-go"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()
	node, _ := spine.CreateNode("example", "publisher_sample", ctx, logger)

	pub, err := spine.NewPublisher[uint32](node, "temperature")
	if err != nil {
		log.Fatal(err)
	}

	var temp uint32 = 0
	for {
		pub.Publish(temp)
		temp++
		time.Sleep(time.Millisecond * 15)
	}

}
