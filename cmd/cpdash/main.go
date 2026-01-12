// Copyright 2020 Jonas Dahlbæk

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// 	http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"cpdash/internal/lib"
	"flag"
	"log"
	"math"
	"strings"
)

func main() {
	args := lib.Args{}

	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lshortfile)

	flag.UintVar(&args.Concurrency, "P", 32, "concurrent requests")
	flag.StringVar(&args.Cpuprofile, "cpuprofile", "", "generate cpu profile")
	flag.StringVar(&args.Memprofile, "memprofile", "", "generate memory profile")
	flag.Uint64Var(&args.Limit, "limit", 10*1024*1024*1024, "download limit")
	flag.Int64Var(&args.BufferLimit, "bufferLimit", 1024*1024*1024, "total buffer memory limit")
	flag.BoolVar(&args.List, "list", false, "only list keys")
	flag.BoolVar(&args.Keys, "keys", false, "print keys")
	flag.BoolVar(&args.Debug, "debug", false, "debug output")
	flag.BoolVar(&args.Force, "f", false, "disable download limit")
	flag.StringVar(&args.AwsProfile, "profile", "", "AWS profile to use")

	flag.Parse()

	if args.Force {
		args.Limit = math.MaxInt64
	}

	nArg := flag.NArg()
	if nArg != 1 {
		log.Fatal("supply precisely one positional argument in the form 's3://<bucket>/<prefix>'")
	}

	urlArg := strings.SplitAfter(flag.Arg(0), "/")
	if len(urlArg) < 4 {
		log.Fatal("url must have the form 's3://<bucket>/<prefix>'")
	}
	if urlArg[0] != "s3:/" {
		log.Fatal("scheme must be s3")
	}

	args.UrlArg = urlArg

	if args.Debug {
		log.Printf("args: %+v", args)
	}

	lib.Run(args)
}
