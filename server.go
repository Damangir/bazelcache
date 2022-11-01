package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/Damangir/bazelcache/bazelcache"
)

var (
	help     bool
	port     int
	maxSize  int
	cacheDir string
)

func init() {
	flag.StringVar(&cacheDir, "dir", "", "local directory to cache data")
	flag.IntVar(&maxSize, "maxsize", 500, "Maximum disk space in Mb the server can consume")
	flag.IntVar(&port, "port", 7777, "port to listen to")
	flag.BoolVar(&help, "help", false, "print help")
}

func run() error {
	c, err := bazelcache.NewFileSystemCache(cacheDir, maxSize<<(10*2))
	if err != nil {
		log.Fatalf("Cannot create a cache folder at %q: %s", cacheDir, err)
	}
	cacheHandler := bazelcache.NewCacheServer(c)
	smx := http.NewServeMux()
	smx.Handle("/", cacheHandler)
	return http.ListenAndServe(fmt.Sprintf(":%d", port), smx)
}

func validateFlags() error {
	if cacheDir == "" {
		return errors.New("no --dir provided")
	}
	if maxSize < 0 {
		return fmt.Errorf("invalid cache size %dMb", maxSize)
	}
	if port < 0 || port > 65535 {
		return fmt.Errorf("invalid port %d", port)
	}
	return nil
}

func main() {
	flag.Parse()
	if help {
		flag.Usage()
		os.Exit(0)
	}
	if err := validateFlags(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		flag.Usage()
		os.Exit(1)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
