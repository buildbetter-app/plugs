package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/buildbetter-app/plugs/tools/lexicon"
)

func main() {
	root := flag.String("root", ".", "repository root")
	flag.Parse()
	findings, err := lexicon.CheckTree(*root)
	if err == nil {
		err = lexicon.CheckError(findings)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("lexicon: clean")
}
