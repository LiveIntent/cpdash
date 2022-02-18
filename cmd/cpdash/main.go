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
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"cpdash/internal/lib"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"

	"golang.org/x/sync/semaphore"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gobwas/glob"
	"github.com/klauspost/compress/zstd"
)

var concurrency uint
var cpuprofile string
var memprofile string
var limit uint64
var list bool
var keys bool
var bufferLimit int64

var bucket string
var prefix string
var globs []glob.Glob
var pattern glob.Glob
var debug bool

var s3Client *s3.Client

var mu sync.Mutex

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lshortfile)

	flag.UintVar(&concurrency, "P", 32, "concurrent requests")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "generate cpu profile")
	flag.StringVar(&memprofile, "memprofile", "", "generate memory profile")
	flag.Uint64Var(&limit, "limit", 10*1024*1024*1024, "download limit")
	flag.Int64Var(&bufferLimit, "bufferLimit", 1024*1024*1024, "total buffer memory limit")
	flag.BoolVar(&list, "list", false, "only list keys")
	flag.BoolVar(&keys, "keys", false, "print keys")
	flag.BoolVar(&debug, "debug", false, "debug output")

	force := flag.Bool("f", false, "disable download limit")

	flag.Parse()

	if *force {
		limit = math.MaxInt64
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

	bucket = urlArg[2][:len(urlArg[2])-1]
	dirs := urlArg[3:]
	path := strings.Join(dirs, "")

	pattern = glob.MustCompile(path, '/')

	prefix = ""
	globbed := false
	for i, dir := range dirs {
		if strings.Contains(dir, "**") {
			globs = append(globs, glob.MustCompile(strings.Join(dirs[:i+1], ""), '/'))
			break
		}
		if !globbed {
			prefix += dir
			for _, c := range []byte{'{', '[', '*', '\\', '?'} {
				idx := strings.IndexByte(prefix, c)
				if idx != -1 {
					if !globbed && prefix[len(prefix)-1] == '/' {
						prefix = prefix[:len(prefix)-1]
					}
					globbed = true
					prefix = prefix[:idx]
				}
			}
		}
		if globbed {
			globs = append(globs, glob.MustCompile(strings.Join(dirs[:i+1], ""), '/'))
		}
	}

	if debug {
		log.Printf("prefix: %s", prefix)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	s3Client = s3.NewFromConfig(cfg)
}

func main() {
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

	bytesDownloaded, pooled, streamed := lib.Produce(bucket, prefix, limit, s3Client, globs, pattern, list, debug, bufferLimit)

	var wg sync.WaitGroup

	wg.Add(1)
	go func(streamed <-chan lib.Object) {
		defer wg.Done()
		for obj := range streamed {
			consumeSequential(obj)
		}
	}(streamed)

	sem := semaphore.NewWeighted(bufferLimit)
	for i := uint(0); i < concurrency; i++ {
		wg.Add(1)
		go func(pooled <-chan lib.Object) {
			defer wg.Done()

			consume(pooled, sem)
		}(pooled)
	}
	wg.Wait()

	if *bytesDownloaded > limit {
		log.Printf("exceeded download limit %v, downloaded %v bytes", limit, bytesDownloaded)
		os.Exit(1)
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

func consume(pooled <-chan lib.Object, sem *semaphore.Weighted) {
	for obj := range pooled {
		sem.Acquire(context.TODO(), obj.Size)
		buffer := manager.NewWriteAtBuffer(make([]byte, obj.Size))

		_, dErr := manager.NewDownloader(s3Client).Download(context.TODO(), buffer, &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &obj.Key,
		})
		if dErr != nil {
			log.Panicf("failed to download file s3://%s/%s, %v", bucket, obj.Key, dErr)
		}

		logContent(obj.Key, bytes.NewBuffer(buffer.Bytes()))
		sem.Release(obj.Size)
	}
}

func consumeSequential(obj lib.Object) {
	object, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &obj.Key,
	})
	if err != nil {
		log.Panicf("failed to download file s3://%s/%s, %v", bucket, obj.Key, err)
	}
	body := object.Body
	defer body.Close()

	logContent(obj.Key, body)
}

var gzipReader = new(gzip.Reader)
var zstdReader, _ = zstd.NewReader(nil)
var peekReader = bufio.NewReaderSize(nil, 4)
var copyBuf = make([]byte, 1<<20)

var gzipMagic = []byte{0x1f, 0x8b, 0x08}
var zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

func logContent(key string, body io.Reader) {
	mu.Lock()
	defer mu.Unlock()

	peekReader.Reset(body)
	head, err := peekReader.Peek(4)
	if err != nil && err != io.EOF {
		log.Fatalf("failed while reading s3://%s/%s: %s", bucket, key, err)
	}

	var reader io.Reader
	switch {
	case len(head) == 4 && bytes.Equal(head, zstdMagic):
		err := zstdReader.Reset(peekReader)
		if err != nil {
			log.Fatalf("failed to download file s3://%s/%s after zstd verification, %v", bucket, key, err)
		}
		reader = zstdReader
	case len(head) >= 3 && bytes.Equal(head[:3], gzipMagic):
		err := gzipReader.Reset(peekReader)
		if err != nil {
			log.Fatalf("failed to download file s3://%s/%s after gzip verification, %v", bucket, key, err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	default:
		reader = peekReader
	}

	if keys {
		fmt.Printf("---------- content of s3://%s/%s ----------\n", bucket, key)
	}

	nw, err := io.CopyBuffer(os.Stdout, reader, copyBuf)
	if err != nil {
		log.Fatalf("failed while copying s3://%s/%s to stdout, wrote %d bytes: %s", bucket, key, nw, err)
	}
}
