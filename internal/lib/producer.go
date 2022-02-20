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
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gobwas/glob"
)

type GlobPattern struct {
	bucket         string
	globFreePrefix string
	pattern        glob.Glob
	globs          []glob.Glob
}

func newGlobPattern(urlArg []string) GlobPattern {
	prefix := ""
	globs := []glob.Glob{}
	dirs := urlArg[3:]
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

	path := strings.Join(dirs, "")

	return GlobPattern{
		bucket:         urlArg[2][:len(urlArg[2])-1],
		globFreePrefix: prefix,
		pattern:        glob.MustCompile(path, '/'),
		globs:          globs,
	}
}

type producer struct {
	args            Args
	globPattern     GlobPattern
	bytesDownloaded *uint64
	pooled          chan<- Object
	streamed        chan<- Object
	s3Client        *s3.Client
}

type Object struct {
	Bucket string
	Key    string
	Size   int64
}

func produce(args Args, globPattern GlobPattern, s3Client *s3.Client) (*uint64, <-chan Object, <-chan Object) {
	pooled := make(chan Object)
	streamed := make(chan Object)
	bytesDownloaded := new(uint64)
	p := producer{
		args:            args,
		globPattern:     globPattern,
		bytesDownloaded: bytesDownloaded,
		pooled:          pooled,
		streamed:        streamed,
		s3Client:        s3Client,
	}

	go func() {
		defer close(pooled)
		defer close(streamed)
		p.produce(globPattern.globFreePrefix, globPattern.globs)
	}()

	return bytesDownloaded, pooled, streamed
}

func (p *producer) produce(prefix string, globs []glob.Glob) {
	var delimiter *string
	if len(globs) > 1 {
		delimiter = aws.String("/")
	}
	input := s3.ListObjectsV2Input{
		Bucket:    &p.globPattern.bucket,
		Prefix:    &prefix,
		Delimiter: delimiter,
	}
	if p.args.Debug {
		log.Printf("input: %+v", input)
	}
	continuationToken, ok := p.walk_page(&input, globs)
	for ok {
		input.ContinuationToken = continuationToken
		continuationToken, ok = p.walk_page(&input, globs)
	}
}

func (p *producer) walk_page(input *s3.ListObjectsV2Input, globs []glob.Glob) (*string, bool) {
	result, err := p.s3Client.ListObjectsV2(context.TODO(), input)
	if err != nil {
		log.Panic(err)
	}

	if len(globs) <= 1 {
		bucket := p.globPattern.bucket
		for _, object := range result.Contents {
			key := *object.Key
			size := object.Size
			if p.globPattern.pattern.Match(key) {
				if p.args.List {
					fmt.Printf("s3://%s/%s\n", bucket, key)
				} else {
					if size < p.args.BufferLimit {
						p.pooled <- Object{bucket, key, size}
					} else {
						p.streamed <- Object{bucket, key, size}
					}
					*p.bytesDownloaded += uint64(size)
					if *p.bytesDownloaded > p.args.Limit {
						return nil, false
					}
				}
			}
		}
	} else {
		for _, commonPrefix := range result.CommonPrefixes {
			prefix := *commonPrefix.Prefix
			if globs[0].Match(prefix) {
				if p.args.Debug {
					log.Printf("matched common prefix %s", prefix)
				}
				p.produce(prefix, globs[1:])
			}
		}
	}

	return result.NextContinuationToken, result.IsTruncated
}
