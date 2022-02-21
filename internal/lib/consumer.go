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

package lib

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/zstd"
)

func consume(obj Object, s3Client *s3.Client, keys bool) {
	buffer := manager.NewWriteAtBuffer(make([]byte, obj.Size))

	_, dErr := manager.NewDownloader(s3Client).Download(context.TODO(), buffer, &s3.GetObjectInput{
		Bucket: &obj.Bucket,
		Key:    &obj.Key,
	})
	if dErr != nil {
		log.Panicf("failed to download file s3://%s/%s, %v", obj.Bucket, obj.Key, dErr)
	}

	logContent(obj.Bucket, obj.Key, bytes.NewBuffer(buffer.Bytes()), os.Stdout, keys)
}

func consumeSequential(obj Object, s3Client *s3.Client, keys bool) {
	object, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &obj.Bucket,
		Key:    &obj.Key,
	})
	if err != nil {
		log.Panicf("failed to download file s3://%s/%s, %v", obj.Bucket, obj.Key, err)
	}
	body := object.Body
	defer body.Close()

	logContent(obj.Bucket, obj.Key, body, os.Stdout, keys)
}

var gzipReader = new(gzip.Reader)
var zstdReader, _ = zstd.NewReader(nil)
var peekReader = bufio.NewReaderSize(nil, 4)
var copyBuf = make([]byte, 1<<20)

var gzipMagic = []byte{0x1f, 0x8b, 0x08}
var zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

var mu sync.Mutex

func logContent(bucket string, key string, body io.Reader, output io.Writer, keys bool) {
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

	nw, err := io.CopyBuffer(output, reader, copyBuf)
	if err != nil {
		log.Fatalf("failed while copying s3://%s/%s to stdout, wrote %d bytes: %s", bucket, key, nw, err)
	}
}
