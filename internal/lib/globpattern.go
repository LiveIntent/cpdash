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
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gobwas/glob"
)

type GlobPattern struct {
	bucket          string
	globFreePrefix  string
	pattern         glob.Glob
	globs           []glob.Glob
	counts          []int
	bytesDownloaded *uint64
}

func newGlobPattern(urlArg []string) GlobPattern {
	prefix := ""
	globs := []glob.Glob{}
	counts := []int{}
	dirs := urlArg[3:]

	globbed := false
	for i, dir := range dirs {
		if strings.Contains(dir, "**") {
			globs = append(globs, glob.MustCompile(strings.Join(dirs[:i+1], ""), '/'))
			counts = append(counts, -1)
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
			pattern := strings.Join(dirs[:i+1], "")
			counts = append(counts, strings.Count(pattern, "/"))
			globs = append(globs, glob.MustCompile(pattern, '/'))
		}
	}

	path := strings.Join(dirs, "")
	bytesDownloaded := new(uint64)

	return GlobPattern{
		bucket:          urlArg[2][:len(urlArg[2])-1],
		globFreePrefix:  prefix,
		pattern:         glob.MustCompile(path, '/'),
		globs:           globs,
		counts:          counts,
		bytesDownloaded: bytesDownloaded,
	}
}

func (g GlobPattern) filterContents(contents []types.Object, args Args) (pooled []Object, streamed []Object) {
	for _, object := range contents {
		key := *object.Key
		if !g.pattern.Match(key) {
			continue
		}
		if *object.Size < args.BufferLimit {
			pooled = append(pooled, Object{Bucket: g.bucket, Key: key, Size: *object.Size})
		} else {
			streamed = append(streamed, Object{Bucket: g.bucket, Key: key, Size: *object.Size})
		}
		*g.bytesDownloaded += uint64(*object.Size)
		if *g.bytesDownloaded > args.Limit {
			break
		}
	}
	return
}

func (g GlobPattern) filterCommonPrefixes(commonPrefixes []types.CommonPrefix, prefix string) (prefixes []string) {
	count := strings.Count(prefix, "/") + 1
	for i, pattern := range g.globs {
		if g.counts[i] == -1 || g.counts[i] == count {
			for _, commonPrefix := range commonPrefixes {
				prefix := *commonPrefix.Prefix
				if pattern.Match(prefix) {
					prefixes = append(prefixes, prefix)
				}
			}
		}
	}
	return
}
