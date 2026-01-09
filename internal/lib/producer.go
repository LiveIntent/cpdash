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
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type producer struct {
	args        Args
	globPattern GlobPattern
	pooled      chan<- Object
	streamed    chan<- Object
	s3Client    *s3.Client
}

type Object struct {
	Bucket string
	Key    string
	Size   int64
}

func produce(args Args, s3Client *s3.Client) (*uint64, <-chan Object, <-chan Object) {
	globPattern := newGlobPattern(args.UrlArg)
	pooled := make(chan Object)
	streamed := make(chan Object)
	p := producer{
		args:        args,
		globPattern: globPattern,
		pooled:      pooled,
		streamed:    streamed,
		s3Client:    s3Client,
	}

	go func() {
		defer close(pooled)
		defer close(streamed)
		p.produce(globPattern.globFreePrefix)
	}()

	return globPattern.bytesDownloaded, pooled, streamed
}

func (p *producer) produce(prefix string) {
	input := s3.ListObjectsV2Input{
		Bucket:    &p.globPattern.bucket,
		Prefix:    &prefix,
		Delimiter: aws.String("/"),
	}
	if p.args.Debug {
		log.Printf("input: %+v", input)
	}
	continuationToken, ok := p.walk_page(&input)
	for ok {
		input.ContinuationToken = continuationToken
		continuationToken, ok = p.walk_page(&input)
	}
}

func (p *producer) walk_page(input *s3.ListObjectsV2Input) (*string, bool) {
	result, err := p.s3Client.ListObjectsV2(context.TODO(), input)
	if err != nil {
		log.Panic(err)
	}

	pooled, streamed := p.globPattern.filterContents(result.Contents, p.args)
	for _, object := range pooled {
		if p.args.List {
			fmt.Printf("s3://%s/%s\n", object.Bucket, object.Key)
		} else {
			p.pooled <- object
		}
	}
	for _, object := range streamed {
		if p.args.List {
			fmt.Printf("s3://%s/%s\n", object.Bucket, object.Key)
		} else {
			p.streamed <- object
		}
	}
	if *p.globPattern.bytesDownloaded > p.args.Limit {
		return nil, false
	}

	commonPrefixes := p.globPattern.filterCommonPrefixes(result.CommonPrefixes, *result.Prefix)
	for _, prefix := range commonPrefixes {
		if p.args.Debug {
			log.Printf("matched common prefix %s", prefix)
		}
		p.produce(prefix)
	}

	isTruncated := result.IsTruncated != nil && *result.IsTruncated
	return result.NextContinuationToken, isTruncated
}
