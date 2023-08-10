package main

import (
	"flag"
	"go-db-compare/configs"
	"go-db-compare/internal"
	"log"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Parse command line flags: config and strategy
	configFile := flag.String("c", "", "path to config file (e.g. config.yaml)")
	strategy := flag.String("s", "", "strategy [dump, twodumps, live, diff]")
	flag.Parse()

	// Get config
	conf, err := configs.GetConf(*configFile)
	if err != nil {
		return err
	}

	// Run
	if err := internal.RunCompare(conf, *strategy); err != nil {
		return err
	}

	return nil
}
