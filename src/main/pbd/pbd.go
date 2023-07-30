package main

//
// see directions in pbc.go
//

import (
	"fmt"
	"os"
	"time"

	"github.com/l1nkkk/6.5840/pbservice"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: pbd viewport myport\n")
		os.Exit(1)
	}

	pbservice.StartServer(os.Args[1], os.Args[2])

	for {
		time.Sleep(100 * time.Second)
	}
}