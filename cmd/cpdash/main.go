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
	"fmt"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"

	"golang.org/x/sync/semaphore"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

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

var sess *session.Session = session.Must(session.NewSessionWithOptions(session.Options{
	SharedConfigState: session.SharedConfigEnable,
}))
var downloader = s3manager.NewDownloader(sess)
var s3Client = s3.New(sess)

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

	urlArg, err := url.Parse(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	if urlArg.Scheme != "s3" {
		log.Fatal("scheme must be s3")
	}
	if urlArg.Path == "" {
		log.Fatal("prefix missing in s3 url, must have the form 's3://<bucket>/<prefix>'")
	}
	bucket = urlArg.Host
	path := urlArg.Path[1:]

	pattern = glob.MustCompile(path, '/')

	dirs := strings.SplitAfter(path, "/")
	prefix = ""
	globbed := false
	for i, dir := range dirs {
		if strings.Contains(dir, "**") {
			globs = append(globs, glob.MustCompile(strings.Join(dirs[:i+1], ""), '/'))
			break
		}
		if !globbed {
			prefix += dir
			for _, c := range []byte{'{', '[', '*', '\\'} {
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

	p, objs, sequentialFuture := lib.Produce(bucket, prefix, limit, s3Client, globs, pattern, list, debug)

	sequential := <-sequentialFuture

	if concurrency == 1 || sequential {
		for obj := range objs {
			consumeSequential(obj)
		}
	} else {
		sem := semaphore.NewWeighted(bufferLimit)
		var wg sync.WaitGroup
		for i := uint(0); i < concurrency; i++ {
			wg.Add(1)
			go func(objs <-chan lib.Object) {
				defer wg.Done()

				consume(objs, sem)
			}(objs)
		}
		wg.Wait()
	}

	if p.BytesDownloaded > limit {
		log.Printf("exceeded download limit %v, downloaded %v bytes", limit, p.BytesDownloaded)
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

func consume(objs <-chan lib.Object, sem *semaphore.Weighted) {
	buffer := []byte{}
	bufSize := int64(0)

	for obj := range objs {
		key := obj.Key
		size := obj.Size

		if size > bufferLimit {
			log.Fatalf("*object.Size > bufferLimit: %+v", obj)
		}

		if size > int64(cap(buffer)) {
			buffer = nil
			sem.Release(bufSize)

			bufSize = size
			sem.Acquire(context.TODO(), bufSize)
			buffer = make([]byte, bufSize)
		}
		f := aws.NewWriteAtBuffer(buffer[:0])

		_, dErr := downloader.Download(f, &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		if dErr != nil {
			log.Panicf("failed to download file s3://%s/%s, %v", bucket, key, dErr)
		}

		logContent(key, bytes.NewBuffer(f.Bytes()))
	}

	buffer = nil
	sem.Release(bufSize)
}

func consumeSequential(obj lib.Object) {
	key := obj.Key
	object, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		log.Panicf("failed to download file s3://%s/%s, %v", bucket, key, err)
	}
	body := object.Body
	defer body.Close()

	logContent(key, body)
}

var gzipReader = new(gzip.Reader)
var zstdReader, _ = zstd.NewReader(nil)
var headBuffer = bytes.NewBuffer(make([]byte, 0, 256))

type onlyWriter struct {
	stdout *os.File
}

func (o onlyWriter) Write(p []byte) (int, error) {
	return o.stdout.Write(p)
}

var zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

func logContent(key string, body io.Reader) {
	mu.Lock()
	defer mu.Unlock()

	headBuffer.Reset()
	head := io.TeeReader(body, headBuffer)

	var reader io.Reader
	err := gzipReader.Reset(head)
	switch err {
	case nil:
		err := gzipReader.Reset(io.MultiReader(headBuffer, body))
		if err != nil {
			log.Fatalf("failed to download file s3://%s/%s after gzip verification, %v", bucket, key, err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	case io.EOF:
	case gzip.ErrHeader, io.ErrUnexpectedEOF:
		if headBuffer.Len() >= 4 && bytes.Equal(headBuffer.Bytes()[:4], zstdMagic) {
			err := zstdReader.Reset(io.MultiReader(headBuffer, body))
			if err != nil {
				log.Fatalf("failed to download file s3://%s/%s after zstd verification, %v", bucket, key, err)
			}
			reader = zstdReader
		} else {
			reader = io.MultiReader(headBuffer, body)
		}
	default:
		log.Fatalf("failed while reading s3://%s/%s: %s", bucket, key, err)
	}

	if keys {
		fmt.Printf("---------- content of s3://%s/%s ----------\n", bucket, key)
	}

	if reader != nil {
		err := copyBuffer(onlyWriter{os.Stdout}, reader)
		if err != nil {
			log.Fatalf("failed while copying %s to stdout: %s", key, err)
		}
	}
}

var buf = make([]byte, 32*1024)

func copyBuffer(dst io.Writer, src io.Reader) (err error) {
	var last byte

	// borrowed mostly from io.copyBuffer
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
			last = buf[nr-1]
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	if err != nil {
		return
	}
	if last != '\n' {
		nw, err := dst.Write([]byte{'\n'})
		if err != nil || nw != 1 {
			log.Fatalf("failed to write missing newline to stdout, wrote %d bytes: %s", nw, err)
		}
	}

	return
}
