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
	"log"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type producer struct {
	bucket          string
	BytesDownloaded int64
	channel         chan<- string
	limit           int64
	prefix          string
	svc             *s3.S3
}

func Produce(bucket string, prefix string, concurrency uint, limit int64, sess *session.Session) (*producer, <-chan string) {
	channel := make(chan string, concurrency)

	p := producer{
		bucket:          bucket,
		BytesDownloaded: 0,
		channel:         channel,
		limit:           limit,
		prefix:          prefix,
		svc:             s3.New(sess),
	}

	go p.produce()

	return &p, channel
}

func (p *producer) produce() {
	defer close(p.channel)
	input := &s3.ListObjectsV2Input{
		Bucket: &p.bucket,
		Prefix: &p.prefix,
	}
	continuationToken, ok := p.walk_page(input)
	for ok {
		input.ContinuationToken = continuationToken
		continuationToken, ok = p.walk_page(input)
	}
}

func (p *producer) walk_page(input *s3.ListObjectsV2Input) (*string, bool) {
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

	for _, object := range result.Contents {
		p.channel <- *object.Key
		p.BytesDownloaded += *object.Size
		if p.BytesDownloaded > p.limit {
			return nil, false
		}
	}

	return result.NextContinuationToken, *result.IsTruncated
}
