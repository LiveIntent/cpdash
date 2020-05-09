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
	"regexp"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func main() {
	prepend, limit, concurrency, url, res, delimiter, nonRecursive, list := getArgs()
	if url.Scheme != "s3" {
		panic("scheme must be s3")
	}

	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lshortfile)
	bucket := url.Host
	prefix := url.Path[1:]
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	p, channel := lib.Produce(bucket, prefix, concurrency, limit, sess, res, delimiter, nonRecursive, list)
	var consumerWg sync.WaitGroup
	var stdoutLoggerMutex sync.Mutex
	stdoutLogger := log.New(os.Stdout, "", 0)
	downloader := s3manager.NewDownloader(sess)
	for i := uint(0); i < concurrency; i++ {
		consumerWg.Add(1)
		go lib.Consume(bucket, channel, prepend, &consumerWg, stdoutLogger, &stdoutLoggerMutex, downloader)
	}
	consumerWg.Wait()

	if p.BytesDownloaded > limit {
		log.Printf("exceeded download limit %v, downloaded %v bytes", limit, p.BytesDownloaded)
		os.Exit(1)
	}
}

func getArgs() (bool, uint64, uint, url.URL, []regexp.Regexp, string, bool, bool) {
	concurrency := flag.Uint("P", 32, "concurrent requests")
	delimiter := flag.String("d", "/", "s3 key delimiter")
	limit := flag.Uint64("l", 100*1024*1024, "download limit")
	list := flag.Bool("list", false, "only list keys")
	force := flag.Bool("f", false, "disable download limit")
	nonRecursive := flag.Bool("non-recursive", false, "disable recursive search")
	prepend := flag.Bool("k", false, "print keys")
	flag.Parse()
	if *force {
		*limit = math.MaxInt64
	}
	nArg := flag.NArg()
	if nArg < 1 {
		panic("supply at least one positional argument")
	} else if nArg == 1 {
		url := getUrl(flag.Arg(0))
		re := regexp.MustCompile("")
		res := []regexp.Regexp{*re}
		return *prepend, *limit, *concurrency, *url, res, *delimiter, *nonRecursive, *list
	} else {
		args := flag.Args()
		url := getUrl(args[0])
		res := make([]regexp.Regexp, len(args)-1)
		for i, v := range args[1:] {
			res[i] = *regexp.MustCompile(v)
		}
		return *prepend, *limit, *concurrency, *url, res, *delimiter, *nonRecursive, *list
	}
}

func getUrl(arg string) *url.URL {
	url, err := url.Parse(flag.Arg(0))
	if err != nil {
		panic(err)
	}
	return url
}
