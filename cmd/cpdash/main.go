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
	"runtime"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var concurrency uint
var delimiter string
var limit uint64
var list bool
var nonRecursive bool
var prepend bool

var bucket string
var prefix string
var res []regexp.Regexp

var sess *session.Session = session.Must(session.NewSessionWithOptions(session.Options{
	SharedConfigState: session.SharedConfigEnable,
}))

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lshortfile)

	flag.UintVar(&concurrency, "P", 32, "concurrent requests")
	flag.StringVar(&delimiter, "d", "/", "s3 key delimiter")
	flag.Uint64Var(&limit, "l", 100*1024*1024, "download limit")
	flag.BoolVar(&list, "list", false, "only list keys")
	flag.BoolVar(&nonRecursive, "non-recursive", false, "disable recursive search")
	flag.BoolVar(&prepend, "k", false, "print keys")

	force := flag.Bool("f", false, "disable download limit")

	flag.Parse()

	if *force {
		limit = math.MaxInt64
	}

	url, err := url.Parse(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	if url.Scheme != "s3" {
		log.Fatal("scheme must be s3")
	}
	if url.Path == "" {
		log.Fatal("prefix missing in s3 url, must have the form 's3://<bucket>/<prefix>'")
	}
	bucket = url.Host
	prefix = url.Path[1:]

	nArg := flag.NArg()
	if nArg < 1 {
		log.Fatal("supply at least one positional argument")
	} else if nArg == 1 {
		re := regexp.MustCompile("")
		res = []regexp.Regexp{*re}
	} else {
		args := flag.Args()
		res = make([]regexp.Regexp, len(args)-1)
		for i, v := range args[1:] {
			res[i] = *regexp.MustCompile(v)
		}
	}
}

func main() {
	runtime.GOMAXPROCS(1)

	p, keys := lib.Produce(bucket, prefix, limit, sess, res, delimiter, nonRecursive, list)

	var mu sync.Mutex
	logger := log.New(os.Stdout, "", 0)
	downloader := s3manager.NewDownloader(sess)

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	for key := range keys {
		sem <- struct{}{}
		wg.Add(1)
		go func(key string) {
			lib.Consume(bucket, key, prepend, logger, &mu, downloader)
			wg.Done()
			<-sem
		}(key)
	}
	wg.Wait()

	if p.BytesDownloaded > limit {
		log.Printf("exceeded download limit %v, downloaded %v bytes", limit, p.BytesDownloaded)
		os.Exit(1)
	}
}
