package main

import (
	"flag"
	"os"
	"net/http"
)

var Name = "go-json-mdq"
var Version = "1.0.0"

var indexPath = flag.String("index", ".thiss", "index path")
var metadata = flag.String("metadata", os.Getenv("METADATA"), "json metadata")
var bindAddr = flag.String("bind", ":3000", "http listen address")
var batchSize = flag.Int("batchSize", 100, "batch size for indexing")
var serve = flag.Bool("serve", true, "start server (or only index and exit")
var rebuild = flag.Bool("rebuild", false, "force re-index")

func main() {
	flag.Parse()
	if *rebuild {
		os.RemoveAll(*indexPath)
	}
	md := NewMetadata(*metadata, *indexPath)
	if ! *serve {
		os.Exit(0)
	}

	http.Handle("/", md.NewAPI())
	log.Printf("Listening on %v", *bindAddr)
	log.Fatal(http.ListenAndServe(*bindAddr, nil))
}
