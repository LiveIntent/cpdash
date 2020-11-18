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
	"regexp"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type producer struct {
	bucket           string
	BytesDownloaded  uint64
	channel          chan<- Object
	limit            uint64
	svc              *s3.S3
	delimiter        string
	nonRecursive     bool
	list             bool
	sequentialFuture chan<- bool
	objects          uint
}

type Object struct {
	Key  string
	Size int64
}

func Produce(bucket string, prefix string, limit uint64, sess *session.Session, res []regexp.Regexp, delimiter string, nonRecursive bool, list bool) (*producer, <-chan Object, <-chan bool) {
	channel := make(chan Object, 2)
	sequentialFuture := make(chan bool)

	p := producer{
		bucket:           bucket,
		BytesDownloaded:  0,
		channel:          channel,
		limit:            limit,
		svc:              s3.New(sess),
		delimiter:        delimiter,
		nonRecursive:     nonRecursive,
		list:             list,
		sequentialFuture: sequentialFuture,
	}

	go p.produce(prefix, res, true)

	return &p, channel, sequentialFuture
}

func (p *producer) produce(prefix string, res []regexp.Regexp, root bool) {
	if root {
		defer close(p.channel)
	}
	var delimiter *string
	if len(res) > 1 || p.nonRecursive {
		delimiter = &p.delimiter
	} else {
		delimiter = nil
	}
	input := &s3.ListObjectsV2Input{
		Bucket:    &p.bucket,
		Prefix:    &prefix,
		Delimiter: delimiter,
	}
	continuationToken, ok := p.walk_page(input, res)
	for ok {
		input.ContinuationToken = continuationToken
		continuationToken, ok = p.walk_page(input, res)
	}
	if root && p.objects < 2 {
		p.sequentialFuture <- true
	}
}

func (p *producer) walk_page(input *s3.ListObjectsV2Input, res []regexp.Regexp) (*string, bool) {
	inputPrefix := *input.Prefix
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

	if len(res) <= 1 {
		for _, object := range result.Contents {
			key := *object.Key
			size := *object.Size
			if size < 0 {
				log.Fatalf("*object.Size < 0: %+v", object)
			}
			if res[0].MatchString(key[len(inputPrefix):]) {
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
			if res[0].MatchString(prefix[len(inputPrefix):]) {
				p.produce(prefix, res[1:], false)
			}
		}
	}

	return result.NextContinuationToken, *result.IsTruncated
}
