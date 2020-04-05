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
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type consumer struct {
	bucket            string
	channel           <-chan string
	downloader        *s3manager.Downloader
	prepend           bool
	stdoutLogger      *log.Logger
	stdoutLoggerMutex *sync.Mutex
	wg                *sync.WaitGroup
}

func Consume(bucket string, channel <-chan string, prepend bool, wg *sync.WaitGroup, stdoutLogger *log.Logger, stdoutLoggerMutex *sync.Mutex, downloader *s3manager.Downloader) {
	c := consumer{
		bucket:            bucket,
		channel:           channel,
		downloader:        downloader,
		prepend:           prepend,
		stdoutLogger:      stdoutLogger,
		stdoutLoggerMutex: stdoutLoggerMutex,
		wg:                wg,
	}
	c.consume()
}

func (c *consumer) consume() {
	defer c.wg.Done()

	for key := range c.channel {
		f := aws.NewWriteAtBuffer([]byte{})
		_, dErr := c.downloader.Download(f, &s3.GetObjectInput{
			Bucket: &c.bucket,
			Key:    &key,
		})
		if dErr != nil {
			log.Panicf("failed to download file, %v", dErr)
		}

		reader, err := gzip.NewReader(bytes.NewBuffer(f.Bytes()))
		if err != nil {
			switch err.Error() {
			case "EOF":
			case "gzip: invalid header":
				c.logContent(key, f.Bytes())
			default:
				log.Panic(err)
			}
			continue
		}
		defer reader.Close()

		content, err := ioutil.ReadAll(reader)
		if err != nil {
			log.Panic(err)
		}
		c.logContent(key, content)
	}
}

func (c *consumer) logContent(key string, content []byte) {
	if c.prepend {
		c.stdoutLoggerMutex.Lock()
		c.stdoutLogger.Printf("---------- downloading from key %v ----------", key)
		c.stdoutLogger.Printf("%s", content)
		c.stdoutLoggerMutex.Unlock()
	} else {
		c.stdoutLogger.Printf("%s", content)
	}
}
