package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/poisnoir/spine-go/client-go"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()
	// BUGFIX: spined only accepts the "common" namespace today; "example" registered
	// silently-rejected (CreateNode doesn't check the status byte) and left spinedConn
	// pointing at a connection spined had already closed server-side, so the next
	// registerToSpined call (from NewPublisher) failed with "broken pipe".
	node, _ := spine.CreateNode("common", "publisher_sample", ctx, logger)

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
