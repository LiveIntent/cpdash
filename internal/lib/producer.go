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
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gobwas/glob"
)

type producer struct {
	debug            bool
	bucket           string
	BytesDownloaded  uint64
	channel          chan<- Object
	limit            uint64
	svc              *s3.S3
	pattern          glob.Glob
	list             bool
	sequentialFuture chan<- bool
	objects          uint
}

type Object struct {
	Key  string
	Size int64
}

func Produce(bucket string, prefix string, limit uint64, svc *s3.S3, globs []glob.Glob, pattern glob.Glob, list bool, debug bool) (*producer, <-chan Object, <-chan bool) {
	channel := make(chan Object, 2)
	sequentialFuture := make(chan bool)

	p := producer{
		bucket:           bucket,
		BytesDownloaded:  0,
		channel:          channel,
		limit:            limit,
		svc:              svc,
		pattern:          pattern,
		list:             list,
		sequentialFuture: sequentialFuture,
		debug:            debug,
	}

	go p.produce(prefix, globs, true)

	return &p, channel, sequentialFuture
}

func (p *producer) produce(prefix string, globs []glob.Glob, root bool) {
	if root {
		defer close(p.channel)
	}
	var delimiter *string
	if len(globs) > 1 {
		delimiter = aws.String("/")
	} else {
		delimiter = nil
	}
	input := &s3.ListObjectsV2Input{
		Bucket:    &p.bucket,
		Prefix:    &prefix,
		Delimiter: delimiter,
	}
	if p.debug {
		log.Printf("input: %s", input)
	}
	continuationToken, ok := p.walk_page(input, globs)
	for ok {
		input.ContinuationToken = continuationToken
		continuationToken, ok = p.walk_page(input, globs)
	}
	if root && p.objects < 2 {
		p.sequentialFuture <- true
	}
}

func (p *producer) walk_page(input *s3.ListObjectsV2Input, globs []glob.Glob) (*string, bool) {
	result, err := p.svc.ListObjectsV2(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				log.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
			default:
				log.Println(aerr.Error())
			}
		}
		log.Panic(err)
	}

	if len(globs) <= 1 {
		for _, object := range result.Contents {
			key := *object.Key
			size := *object.Size
			if size < 0 {
				log.Fatalf("*object.Size < 0: %+v", object)
			}
			if p.pattern.Match(key) {
				if p.list {
					fmt.Printf("s3://%s/%s\n", p.bucket, key)
				} else {
					p.objects += 1
					if p.objects == 2 {
						close(p.sequentialFuture)
					}
					p.channel <- Object{key, size}
					p.BytesDownloaded += uint64(size)
					if p.BytesDownloaded > p.limit {
						return nil, false
					}
				}
			}
		}
	} else {
		for _, commonPrefix := range result.CommonPrefixes {
			prefix := *commonPrefix.Prefix
			if globs[0].Match(prefix) {
				if p.debug {
					log.Printf("matched common prefix %s", prefix)
				}
				p.produce(prefix, globs[1:], false)
			}
		}
	}

	return result.NextContinuationToken, *result.IsTruncated
}
