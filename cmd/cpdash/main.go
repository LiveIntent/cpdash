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
	"bytes"
	"compress/gzip"
	"context"
	"cpdash/internal/lib"
	"flag"
	"io/ioutil"
	"log"
	"math"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/sync/semaphore"
)

var concurrency uint
var cpuprofile string
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
var downloader = s3manager.NewDownloader(sess)

var mu sync.Mutex
var logger = log.New(os.Stdout, "", 0)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lshortfile)

	flag.UintVar(&concurrency, "P", 32, "concurrent requests")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "generate cpu profile")
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

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	p, keys := lib.Produce(bucket, prefix, limit, sess, res, delimiter, nonRecursive, list)

	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(int64(concurrency))
	for obj := range keys {
		sem.Acquire(context.TODO(), 1)
		wg.Add(1)
		go func(obj lib.Object) {
			defer sem.Release(1)
			defer wg.Done()

			consume(obj.Key, obj.Size)
		}(obj)
	}
	wg.Wait()

	if p.BytesDownloaded > limit {
		log.Printf("exceeded download limit %v, downloaded %v bytes", limit, p.BytesDownloaded)
		os.Exit(1)
	}
}

func consume(key string, size uint) {
	f := aws.NewWriteAtBuffer(make([]byte, size))

	_, dErr := downloader.Download(f, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if dErr != nil {
		log.Panicf("failed to download file, %v", dErr)
	}

	reader, err := gzip.NewReader(bytes.NewBuffer(f.Bytes()))
	if err != nil {
		switch err.Error() {
		case "EOF":
			logContent(key, []byte{})
		case "gzip: invalid header", "unexpected EOF":
			logContent(key, f.Bytes())
		default:
			log.Panicf("failed while reading %s: %s", key, err)
		}
		return
	}
	defer reader.Close()

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Panic(err)
	}
	logContent(key, content)
}

func logContent(key string, content []byte) {
	if prepend && len(content) > 0 {
		mu.Lock()
		defer mu.Unlock()
	}
	if prepend {
		logger.Printf("---------- content of s3://%v/%v ----------", bucket, key)
	}
	if len(content) > 0 {
		logger.Printf("%s", content)
	}
}
