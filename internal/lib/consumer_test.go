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
	"io"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func createGzipped(text string) io.Reader {
	compressed := bytes.NewBuffer(nil)

	gzipWriter := gzip.NewWriter(compressed)
	_, err := gzipWriter.Write([]byte(text))
	if err != nil {
		panic(err)
	}
	gzipWriter.Close()

	return compressed
}

func createZstdded(text string) io.Reader {
	compressed := bytes.NewBuffer(nil)

	zstdWriter, err := zstd.NewWriter(compressed)
	if err != nil {
		panic(err)
	}
	_, err = zstdWriter.Write([]byte(text))
	if err != nil {
		panic(err)
	}
	zstdWriter.Close()

	return compressed
}

func Test_logContent(t *testing.T) {
	type args struct {
		bucket string
		key    string
		body   io.Reader
		output io.Writer
		keys   bool
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty",
			args: args{
				"bucket",
				"key",
				strings.NewReader(""),
				bytes.NewBuffer(nil),
				false,
			},
			want: "",
		},
		{
			name: "text",
			args: args{
				"bucket",
				"key",
				strings.NewReader("text"),
				bytes.NewBuffer(nil),
				false,
			},
			want: "text",
		},
		{
			name: "empty gzip",
			args: args{
				"bucket",
				"key",
				createGzipped(""),
				bytes.NewBuffer(nil),
				false,
			},
			want: "",
		},
		{
			name: "gzip",
			args: args{
				"bucket",
				"key",
				createGzipped("gzip"),
				bytes.NewBuffer(nil),
				false,
			},
			want: "gzip",
		},
		{
			name: "empty zstd",
			args: args{
				"bucket",
				"key",
				createZstdded(""),
				bytes.NewBuffer(nil),
				false,
			},
			want: "",
		},
		{
			name: "zstd",
			args: args{
				"bucket",
				"key",
				createZstdded("zstd"),
				bytes.NewBuffer(nil),
				false,
			},
			want: "zstd",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logContent(tt.args.bucket, tt.args.key, tt.args.body, tt.args.output, tt.args.keys)
			output, _ := tt.args.output.(*bytes.Buffer)
			if output.String() != tt.want {
				t.Errorf("found %s expected %s", output.String(), tt.want)
			}
		})
	}
}
