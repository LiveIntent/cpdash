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

func Consume(bucket string, key string, prepend bool, logger *log.Logger, mu *sync.Mutex, downloader *s3manager.Downloader) {
	f := aws.NewWriteAtBuffer([]byte{})
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
			logContent(bucket, key, []byte{}, prepend, logger, mu)
		case "gzip: invalid header":
			logContent(bucket, key, f.Bytes(), prepend, logger, mu)
		default:
			log.Panic(err)
		}
		return
	}
	defer reader.Close()

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Panic(err)
	}
	logContent(bucket, key, content, prepend, logger, mu)
}

func logContent(bucket string, key string, content []byte, prepend bool, logger *log.Logger, mu *sync.Mutex) {
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
