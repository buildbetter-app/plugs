package main

import (
	"os"

	"github.com/buildbetter-app/plugs/tools/census"
)

func main() { os.Exit(census.RunPaired(os.Args[1:], os.Stdout, os.Stderr)) }
