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
	"net/url"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
)

func main() {
	prepend, limit, concurrency, url := getArgs()
	if url.Scheme != "s3" {
		panic("scheme must be s3")
	}

	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lshortfile)
	bucket := url.Host
	prefix := url.Path[1:]
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	p, channel := lib.Produce(bucket, prefix, concurrency, limit, sess)
	var consumerWg sync.WaitGroup
	var stdoutLoggerMutex sync.Mutex
	for i := uint(0); i < concurrency; i++ {
		consumerWg.Add(1)
		go lib.Consume(bucket, channel, prepend, &consumerWg, &stdoutLoggerMutex, sess)
	}
	consumerWg.Wait()

	if p.BytesDownloaded > limit {
		log.Printf("exceeded download limit %v, downloaded %v bytes", limit, p.BytesDownloaded)
		os.Exit(1)
	}
}

func getArgs() (bool, int64, uint, url.URL) {
	concurrency := flag.Uint("P", 32, "concurrent requests")
	limit := flag.Int64("l", 100*1024*1024, "download limit")
	force := flag.Bool("f", false, "disable download limit")
	prepend := flag.Bool("k", false, "print keys")
	flag.Parse()
	if *limit < 0 {
		log.Println("download limit must be non-negative")
		os.Exit(1)
	}
	if *force {
		*limit = math.MaxInt64
	}
	switch flag.NArg() {
	case 1:
		url, err := url.Parse(flag.Arg(0))
		if err != nil {
			panic(err)
		}
		return *prepend, *limit, *concurrency, *url
	default:
		panic("supply precisely one cli argument")
	}
}
